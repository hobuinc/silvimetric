import numpy as np
import signal
import datetime
import copy
from typing_extensions import Generator
import pandas as pd
import itertools

from dask.distributed import CancelledError
from distributed.client import _get_global_client as get_client

from dask.delayed import delayed
import dask.bag as db
from dask import compute, persist
from dask.distributed import futures_of, as_completed

from .. import Extents, Storage, Data, ShatterConfig, Metric
from ..resources.taskgraph import Graph


def final(
    config: ShatterConfig,
    storage: Storage,
    finished: bool = False,
):
    # modify config to reflect result of shattter process
    config.log.debug('Saving shatter metadata')
    config.mbr = storage.mbrs(config.timestamp)
    config.end_time = datetime.datetime.now().timestamp() * 1000
    config.finished = finished
    storage.save_shatter_meta(config)

    storage.consolidate(config.timestamp)
    storage.vacuum()


def get_data(extents: Extents, filename: str, storage: Storage) -> pd.DataFrame:
    """
    Execute pipeline and retrieve point cloud data for this extent

    :param extents: :class:`silvimetric.resources.extents.Extents` being used.
    :param filename: Path to either PDAL pipeline or point cloud.
    :param storage: :class:`silvimetric.resources.storage.Storage` database.
    :return: Point data array from PDAL.
    """

    attrs = [*[a.name for a in storage.get_attributes()], 'xi', 'yi']
    data = Data(filename, storage.config, bounds=extents.bounds)
    p = data.pipeline
    data.execute(allowed_dims = [*attrs, 'X', 'Y'])

    points = p.get_dataframe(0)
    points = points.loc[points.Y < extents.bounds.maxy]
    points = points.loc[points.Y >= extents.bounds.miny]
    points = points.loc[points.X >= extents.bounds.minx]
    points = points.loc[points.X < extents.bounds.maxx][attrs]

    points.loc[:, 'xi'] = np.floor(points.xi)
    # ceil for y because origin is at top left
    points.loc[:, 'yi'] = np.ceil(points.yi)
    return points


def run_graph(data_in: pd.DataFrame, metrics: list[Metric]) -> pd.DataFrame:
    """
    Run DataFrames through metric processes
    """
    graph = Graph(metrics)

    return graph.run(data_in)


def agg_list(data_in: pd.DataFrame, proc_num: int) -> pd.DataFrame:
    """
    Make variable-length point data attributes into lists
    """
    # groupby won't set index on an empty array
    if data_in.empty:
        return data_in.set_index(['xi', 'yi'])

    old_dtypes = data_in.dtypes
    xyi_dtypes = {'xi': np.float64, 'yi': np.float64}
    o = np.dtype('O')
    first_col_name = data_in.columns[0]
    col_dtypes = {a: o for a in data_in.columns if a not in ['xi', 'yi']}

    coerced = data_in.astype(col_dtypes | xyi_dtypes)
    gb = coerced.groupby(['xi', 'yi'], sort=False)
    counts_df = gb[first_col_name].agg('count').rename('count')
    listed = (
        gb.agg(lambda x: np.array(x, old_dtypes[x.name]))
        .join(counts_df)
        .assign(shatter_process_num=proc_num)
    )

    return listed


def join(list_data: pd.DataFrame, metric_data: pd.DataFrame) -> pd.DataFrame:
    """
    Join the list data and metric DataFrames together.
    """
    return list_data.join(metric_data).reset_index()


def write(
    data_in: pd.DataFrame, storage: Storage, timestamp: tuple[int, int]
) -> int:
    """
    Write cell data to database

    :param data_in: Data to be written to database.
    :param tdb: TileDB write stream.
    :return: Number of points written.
    """
    if data_in.empty:
        return 0

    storage.write(data_in, timestamp)

    pc = data_in['count'].sum().item()
    p = copy.deepcopy(pc)

    del pc, data_in
    return p


@delayed
def do_one(leaf: Extents, config: ShatterConfig, storage: Storage) -> db.Bag:
    """Create dask bags and the order of operations."""

    # remove any extents that have already been done, only skip if full overlap
    if config.mbr:
        if not all(leaf.disjoint_by_mbr(m) for m in config.mbr):
            return 0

    points = get_data(leaf, config.filename, storage)
    listed_data = agg_list(points, config.time_slot)
    metric_data = run_graph(points, storage.get_metrics())
    joined_data = join(listed_data, metric_data)
    point_count = write(joined_data, storage, config.timestamp)

    del joined_data, metric_data, listed_data, points

    return point_count


Leaves = Generator[Extents, None, None]


def run(leaves: Leaves, config: ShatterConfig, storage: Storage) -> int:
    """
    Coordinate running of shatter process and handle any interruptions

    :param leaves: Generator of Leaf nodes.
    :param config: :class:`silvimetric.resources.config.ShatterConfig`
    :param storage: :class:`silvimetric.resources.storage.Storage`
    :return: Number of points processed.
    """
    ## If dask is distributed, use the futures feature
    leaves = [delayed(leaf) for leaf in leaves]
    storage = delayed(storage)

    processes = [do_one(leaf, config, storage) for leaf in leaves]
    dc = get_client()
    if dc is not None:
        count = 1
        futures = futures_of(persist(processes))
        for _, pc in as_completed(futures, with_results=True):
            if pc is None:
                continue
            if isinstance(pc, int):
                config.point_count = config.point_count + pc
                count = count + 1
            elif isinstance(pc, BaseException):
                config.log.warning(pc)
            elif isinstance(pc, CancelledError):
                config.log.warning(pc)
            del pc

    else:
        # Handle non-distributed dask scenarios
        results = compute(*processes)
        pcs = [
            possible_pc for possible_pc in results if possible_pc is not None
        ]
        pc = sum(pcs)
        config.point_count = config.point_count + pc

    return config.point_count


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

    # try to catch sigints and save info about work that has been done so far
    signal.signal(
        signal.SIGINT, lambda signum, frame, c=config, s=storage: final(c, s)
    )

    config.log.debug(f'Shatter Config: {config}')
    config.log.debug(f'Data: {data}')
    config.log.debug(f'Extents: {extents}')

    if not config.time_slot:  # defaults to 0, which is reserved for storage cfg
        config.time_slot = storage.reserve_time_slot()

    if config.bounds is None:
        config.bounds = extents.bounds
    storage.save_shatter_meta(config)

    config.log.debug('Grabbing leaf nodes.')
    es = extents.chunk(data, pc_threshold=(11 * 10**6))

    for e in es:
        if config.tile_size is not None:
            leaves = e.get_leaf_children(config.tile_size)
        else:
            leaves = e.chunk(data, pc_threshold=(600*10**3))

        # Begin main operations
        config.log.debug('Shattering.')
        try:
            run(leaves, config, storage)
            storage.consolidate(config.timestamp)
        except Exception as e:
            final(config, storage)
            raise e
    compute(delayed(final)(config, storage, finished=True))

    return config.point_count
