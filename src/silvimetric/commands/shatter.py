import numpy as np
import signal
import datetime
import copy
from typing_extensions import Generator
import pandas as pd

from dask.distributed import as_completed, futures_of, CancelledError
from distributed.client import _get_global_client as get_client
from dask.delayed import Delayed
import dask.array as da
import dask.bag as db
from dask.diagnostics import ProgressBar

from .. import Extents, Storage, Data, ShatterConfig
from ..resources.taskgraph import Graph


def get_data(extents: Extents, filename: str, storage: Storage):
    """
    Execute pipeline and retrieve point cloud data for this extent

    :param extents: :class:`silvimetric.resources.extents.Extents` being used.
    :param filename: Path to either PDAL pipeline or point cloud.
    :param storage: :class:`silvimetric.resources.storage.Storage` database.
    :return: Point data array from PDAL.
    """

    attrs = [a.name for a in storage.get_attributes()]
    data = Data(filename, storage.config, bounds=extents.bounds)
    p = data.pipeline
    data.execute()

    points = p.get_dataframe(0)
    points = points.loc[points.Y < extents.bounds.maxy]
    points = points.loc[points.Y >= extents.bounds.miny]
    points = points.loc[points.X >= extents.bounds.minx]
    points = points.loc[points.X < extents.bounds.maxx, [*attrs, 'xi', 'yi']]

    points.loc[:, 'xi'] = da.floor(points.xi)
    # ceil for y because origin is at top left
    points.loc[:, 'yi'] = da.ceil(points.yi)
    return points


def run_graph(data_in, metrics):
    """
    Run DataFrames through metric processes
    """
    graph = Graph(metrics)

    return graph.run(data_in)


def agg_list(data_in, proc_num):
    """
    Make variable-length point data attributes into lists
    """
    if data_in is None:
        return None

    old_dtypes = data_in.dtypes
    xyi_dtypes = {'xi': np.float64, 'yi': np.float64}
    o = np.dtype('O')
    first_col_name = data_in.columns[0]
    col_dtypes = {a: o for a in data_in.columns if a not in ['xi', 'yi']}

    coerced = data_in.astype(col_dtypes | xyi_dtypes)
    gb = coerced.groupby(['xi', 'yi'], sort=False)
    listed = gb.agg(lambda x: np.array(x, old_dtypes[x.name]))
    counts_df = gb[first_col_name].agg('count').rename('count')
    listed = listed.join(counts_df)
    listed = listed.assign(shatter_process_num=proc_num)

    return listed


def join(list_data: pd.DataFrame, metric_data):
    """
    Join the list data and metric DataFrames together.
    """
    return list_data.join(metric_data).reset_index()


def write(data_in, storage, timestamp):
    """
    Write cell data to database

    :param data_in: Data to be written to database.
    :param tdb: TileDB write stream.
    :return: Number of points written.
    """

    storage.write(data_in, timestamp)

    pc = data_in['count'].sum().item()
    p = copy.deepcopy(pc)

    del pc, data_in
    return p


Leaves = Generator[Extents, None, None]


def do_one(
    leaf: Extents, config: ShatterConfig, storage: Storage
) -> db.Bag:
    """Create dask bags and the order of operations."""

    timestamp = config.timestamp

    # remove any extents that have already been done, only skip if full overlap
    if config.mbr:
        if not all(leaf.disjoint_by_mbr(m) for m in config.mbr):
            return 0

    points = get_data(leaf, config.filename, storage)
    if points.empty:
        return 0
    metric_data = run_graph(points, storage.get_metrics())
    listed_data = agg_list(points, config.time_slot)
    joined_data = join(listed_data, metric_data)
    point_count = write(joined_data, storage, timestamp)

    return point_count


def run(leaves: Leaves, config: ShatterConfig, storage: Storage) -> int:
    """
    Coordinate running of shatter process and handle any interruptions

    :param leaves: Generator of Leaf nodes.
    :param config: :class:`silvimetric.resources.config.ShatterConfig`
    :param storage: :class:`silvimetric.resources.storage.Storage`
    :return: Number of points processed.
    """

    # Process kill handler. Make sure we write out a config even if we fail.
    def kill_gracefully(signum, frame):
        client = get_client()
        if client is not None:
            client.close()
        end_time = datetime.datetime.now().timestamp() * 1000

        storage.consolidate_shatter(config.timestamp)
        config.end_time = end_time
        config.mbrs = storage.mbrs(config.timestamp)
        config.finished = False
        config.log.info('Saving config before quitting...')

        storage.save_shatter_meta(config)
        config.log.info('Quitting.')

    signal.signal(signal.SIGINT, kill_gracefully)

    leaf_bag: db.Bag = db.from_sequence(leaves)
    processes = leaf_bag.map(do_one, config, storage)

    ## If dask is distributed, use the futures feature
    dc = get_client()
    consolidate_count = 30
    count = 0
    if dc is not None:
        pc_futures = futures_of(processes.persist())
        for batch in as_completed(pc_futures, with_results=True).batches():
            for _, pack in batch:
                if isinstance(pack, CancelledError):
                    continue
                for pc in pack:
                    count += 1
                    if count >= consolidate_count:
                        storage.consolidate_shatter(config.timestamp)
                        count = 0
                    config.point_count = config.point_count + pc
                    del pc

        end_time = datetime.datetime.now().timestamp() * 1000
        config.end_time = end_time
        config.finished = True
        point_count = config.point_count
    else:
        # Handle non-distributed dask scenarios
        with ProgressBar():
            point_count = sum(processes)

    return point_count


def shatter(config: ShatterConfig) -> int:
    """
    Handle setup and running of shatter process.
    Will look for a config that has already been run before and needs to be
    resumed.

    :param config: :class:`silvimetric.resources.config.ShatterConfig`.
    :return: Number of points processed.
    """

    # get start time in milliseconds
    config.start_time = datetime.datetime.now().timestamp() * 1000
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    data = Data(config.filename, storage.config, config.bounds)
    extents = Extents.from_sub(config.tdb_dir, data.bounds)

    config.log.info('Beginning shatter process...')
    config.log.debug(f'Shatter Config: {config}')
    config.log.debug(f'Data: {data}')
    config.log.debug(f'Extents: {extents}')

    if not config.time_slot:  # defaults to 0, which is reserved for storage cfg
        config.time_slot = storage.reserve_time_slot()

    if config.bounds is None:
        config.bounds = extents.bounds

    config.log.debug('Grabbing leaf nodes...')
    if config.tile_size is not None:
        leaves = extents.get_leaf_children(config.tile_size)
    else:
        leaves = extents.chunk(
            data, res_threshold=storage.config.resolution, depth_threshold=30
        )
        leaf_count = len(leaves)
        config.log.debug(f'{leaf_count} tiles to process.')


    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    storage.save_shatter_meta(config)
    try:
        pc = run(leaves, config, storage)
    except Exception as e:
        config.mbr = storage.mbrs(config.timestamp)
        config.finished = False
        storage.save_shatter_meta(config)
        storage.consolidate_shatter(config.timestamp)
        raise e

    # consolidate the fragments in this time slot down to just one
    storage.consolidate_shatter(config.timestamp)
    storage.vacuum()

    # modify config to reflect result of shattter process
    config.log.debug('Saving shatter metadata')
    config.point_count = pc
    config.mbr = storage.mbrs(config.timestamp)
    config.end_time = datetime.datetime.now().timestamp() * 1000
    config.finished = True

    storage.save_shatter_meta(config)
    return pc
