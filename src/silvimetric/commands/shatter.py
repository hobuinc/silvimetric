import numpy as np
import signal
from datetime import datetime
import copy

from typing_extensions import Generator
import pandas as pd

from dask.distributed import CancelledError
from distributed.client import _get_global_client as get_client

from dask.delayed import delayed
import dask.bag as db
from dask import compute, persist
from dask.distributed import futures_of, as_completed, KilledWorker

from .. import Extents, Storage, Data, ShatterConfig, Metric
from ..resources.taskgraph import Graph


def final(
    config: ShatterConfig,
    storage: Storage,
    finished: bool = False,
):
    # do consolidation first so that new fragments are written before
    # the timestamp is changed
    config.log.debug('Final consolidation.')
    storage.consolidate(config.timestamp)
    storage.vacuum()

    # modify config to reflect result of shattter process
    config.log.debug('Saving shatter metadata.')
    config.end_timestamp = int(datetime.now().timestamp() * 1000)
    config.mbr = storage.mbrs(config=config)
    config.finished = finished
    storage.save_shatter_meta(config)



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
    data.execute(allowed_dims=[*attrs, 'X', 'Y'])

    try:
        points = p.get_dataframe(0)
    except IndexError:
        return pd.DataFrame()

    points = points.loc[points.Y < extents.bounds.maxy]
    points = points.loc[points.Y >= extents.bounds.miny]
    points = points.loc[points.X >= extents.bounds.minx]
    points = points.loc[points.X < extents.bounds.maxx][attrs]

    points.loc[:, 'xi'] = np.floor(points.xi).astype(np.int32)
    # ceil for y because origin is at top left
    points.loc[:, 'yi'] = np.ceil(points.yi).astype(np.int32)

    return points


def run_graph(data_in: pd.DataFrame, metrics: list[Metric]) -> pd.DataFrame:
    """
    Run DataFrames through metric processes
    """
    graph = Graph(metrics)

    return graph.run(data_in)


def agg_list(
    data_in: pd.DataFrame, proc_num: int, extents: Extents
) -> pd.DataFrame:
    """
    Make variable-length point data attributes into lists
    """
    # groupby won't set index on an empty array
    if data_in.empty:
        return data_in.set_index(['xi', 'yi'])

    old_dtypes = data_in.dtypes
    xyi_dtypes = {'xi': np.int32, 'yi': np.int32}
    o = np.dtype('O')
    first_col_name = data_in.columns[0]
    cols = [a for a in data_in.columns if a not in ['xi', 'yi']]
    col_dtypes = {a: o for a in cols}

    coerced = data_in.astype(col_dtypes | xyi_dtypes)
    gb = coerced.groupby(['xi', 'yi'], sort=True)
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
    data_in: pd.DataFrame,
    storage: Storage,
    dates: tuple[datetime, datetime],
) -> int:
    """
    Write cell data to database

    :param data_in: Data to be written to database.
    :param storage: SilviMetric Storage object.
    :param dates: Tuple of start and end datetimes for dataset.
    :return: Number of points written.
    """
    if data_in.empty:
        return 0

    storage.write(data_in, dates)

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
    if points.empty:
        return 0
    listed_data = agg_list(points, config.time_slot, leaf)
    metric_data = run_graph(points, storage.get_metrics())
    joined_data = join(listed_data, metric_data)
    point_count = write(joined_data, storage, config.date)

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

    config_arg = delayed(config)
    storage_arg = delayed(storage)

    processes = [do_one(leaf, config_arg, storage_arg) for leaf in leaves]
    proc_dict = {p.key: l for p in processes for l in leaves}
    dc = get_client()
    if dc is not None:
        failures = []
        count = 1
        futures = futures_of(persist(processes))
        res = as_completed(futures, with_results=True, raise_errors=False)
        for future, pc in res:
            if future.status == 'error':
                failures.append(proc_dict[future.key])
                continue

                # oldleaf: Extents = proc_dict[future.key].compute(scheduler='threads')
                # if pc[0] is KilledWorker:
                #     splits = [delayed(s) for s in oldleaf.split()]
                #     add_futures = []
                #     for l in splits:
                #         temp_future= futures_of(persist([do_one(l, config_arg, storage_arg)]))
                #         proc_dict[temp_future.key] = l
                #         add_futures.append(temp_future)
                #     for f in add_futures:
                #         res.add(f)
                # else:
                #     config.log.error(f'Failed task for bounds: {oldleaf.bounds}'
                #         f'with error {pc}')

            if pc is None:
                continue
            if isinstance(pc, int):
                config.point_count = config.point_count + pc
                count = count + 1
            del pc

        if failures:
            print(failures)

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
    config.start_timestamp = int(datetime.now().timestamp() * 1000)

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
    es = extents.chunk(data, pc_threshold=config.split_point_count)

    config.log.debug('Shattering.')
    count = 0
    for e in es:
        count = count + 1
        if config.tile_size is not None:
            leaves = e.get_leaf_children(config.tile_size)
        else:
            leaves = e.chunk(data, pc_threshold=config.tile_point_count)

        config.log.debug(f'Shattering extent #{count}.')
        # Begin main operations
        try:
            run(leaves, config, storage)
            storage.consolidate(config.timestamp)
        except Exception as e:
            final(config, storage)
            raise e

    if get_client() is not None:
        # only do this if we can do it on a distributed scheduler so we
        # can throw it to the background
        persist(delayed(final)(config, storage, finished=True))
    else:
        final(config, storage, finished=True)

    return config.point_count
