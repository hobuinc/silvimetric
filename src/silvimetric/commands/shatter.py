import numpy as np
import signal
import datetime
import copy
from typing_extensions import Generator
import pandas as pd
import tiledb

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
    data = Data(filename, storage.config, bounds=extents.bounds)
    p = data.pipeline
    data.execute()
    return p.get_dataframe(0)


def arrange(points: pd.DataFrame, leaf, attrs: list[str]):
    """
    Arrange data to fit key-value TileDB input format.

    :param data: Tuple of indices and point data array (xis, yis, data).
    :param leaf: :class:`silvimetric.resources.extents.Extent` being used.
    :param attrs: List of attribute names.
    :raises Exception: Missing attribute error.
    :return: None if no work is done, or a tuple of indices and rearranged data.
    """
    if points is None:
        return None
    if points.size == 0:
        return None

    points = points.loc[points.Y < leaf.bounds.maxy]
    points = points.loc[points.Y >= leaf.bounds.miny]
    points = points.loc[points.X >= leaf.bounds.minx]
    points = points.loc[points.X < leaf.bounds.maxx, [*attrs, 'xi', 'yi']]

    if points.size == 0:
        return None

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


def agg_list(data_in, dates):
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
    a = coerced.groupby(['xi', 'yi']).agg(
        lambda x: np.array(x, old_dtypes[x.name])
    )
    return a.assign(
        count=lambda x: [len(z) for z in a[first_col_name]],
        start_datetime=lambda st: [dates[0] for z in a[first_col_name]],
        end_datetime=lambda st: [dates[1] for z in a[first_col_name]],
    )

def join(list_data: pd.DataFrame, metric_data):
    """
    Join the list data and metric DataFrames together.
    """
    if list_data is None or metric_data is None:
        return None

    if isinstance(metric_data, Delayed):
        metric_data = metric_data.compute()

    return list_data.join(metric_data).reset_index()


def write(data_in, storage, timestamp):
    """
    Write cell data to database

    :param data_in: Data to be written to database.
    :param tdb: TileDB write stream.
    :return: Number of points written.
    """

    data_in = data_in.rename(columns={'xi': 'X', 'yi': 'Y'})

    attr_dict = {f'{a.name}': a.dtype for a in storage.config.attrs}
    xy_dict = {'X': data_in.X.dtype, 'Y': data_in.Y.dtype}
    metr_dict = {
        f'{m.entry_name(a.name)}': np.dtype(m.dtype)
        for m in storage.config.metrics
        for a in storage.config.attrs
    }
    dtype_dict = attr_dict | xy_dict | metr_dict

    varlen_types = {a.dtype for a in storage.config.attrs}

    fillna_dict = {
        f'{m.entry_name(a.name)}': m.nan_value
        for m in storage.config.metrics
        for a in storage.config.attrs
    }

    tiledb.from_pandas(
        uri=storage.config.tdb_dir,
        sparse=True,
        dataframe=data_in,
        mode='append',
        timestamp=timestamp,
        column_types=dtype_dict,
        varlen_types=varlen_types,
        fillna = fillna_dict
    )

    pc = data_in['count'].sum().item()
    p = copy.deepcopy(pc)

    del pc, data_in
    return p


Leaves = Generator[Extents, None, None]


def get_processes(
    leaves: Leaves, config: ShatterConfig, storage: Storage
) -> db.Bag:
    """Create dask bags and the order of operations."""

    ## Handle dask bag transitions through work states
    attrs = [a.name for a in storage.config.attrs]
    timestamp = (config.time_slot, config.time_slot)

    # remove any extents that have already been done, only skip if full overlap
    leaf_bag: db.Bag = db.from_sequence(leaves)
    if config.mbr:

        def mbr_filter(one: Extents):
            return all(one.disjoint_by_mbr(m) for m in config.mbr)

        leaf_bag = leaf_bag.filter(mbr_filter)

    def pc_filter(d: pd.DataFrame):
        if d is None:
            return False
        return not d.empty

    points: db.Bag = leaf_bag.map(get_data, config.filename, storage)
    arranged: db.Bag = points.map(arrange, leaf_bag, attrs)
    filtered: db.Bag = arranged.filter(pc_filter)
    metrics: db.Bag = filtered.map(run_graph, storage.config.metrics)
    lists: db.Bag = filtered.map(agg_list, config.date)
    joined: db.Bag = lists.map(join, metrics)
    writes: db.Bag = joined.map(write, storage, timestamp)

    return writes


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

        storage.consolidate_shatter(config.time_slot)
        config.end_time = end_time
        config.mbrs = storage.mbrs(config.time_slot)
        config.finished = False
        config.log.info('Saving config before quitting...')

        storage.saveMetadata('shatter', str(config), config.time_slot)
        config.log.info('Quitting.')

    signal.signal(signal.SIGINT, kill_gracefully)

    processes = get_processes(leaves, config, storage)

    ## If dask is distributed, use the futures feature
    dc = get_client()
    if dc is not None:
        pc_futures = futures_of(processes.persist())
        for batch in as_completed(pc_futures, with_results=True).batches():
            for _, pack in batch:
                if isinstance(pack, CancelledError):
                    continue
                for pc in pack:
                    config.point_count = config.point_count + pc
                    del pc

        end_time = datetime.datetime.now().timestamp() * 1000
        config.end_time = end_time
        config.finished = True
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

    config.log.info('Grabbing leaf nodes...')
    if config.tile_size is not None:
        leaves = extents.get_leaf_children(config.tile_size)
    else:
        leaves = extents.chunk(data, 100)

    # Begin main operations
    config.log.info('Fetching and arranging data...')
    try:
        pc = run(leaves, config, storage)
    except Exception as e:
        config.mbr = storage.mbrs(config.time_slot)
        config.finished = False
        storage.saveMetadata('shatter', str(config), config.time_slot)
        raise e

    # consolidate the fragments in this time slot down to just one
    storage.consolidate_shatter(config.time_slot)

    # modify config to reflect result of shattter process
    config.log.info('Saving shatter metadata')
    config.point_count = pc
    config.mbr = storage.mbrs(config.time_slot)
    config.end_time = datetime.datetime.now().timestamp() * 1000
    config.finished = True

    storage.saveMetadata('shatter', str(config), config.time_slot)
    return pc
