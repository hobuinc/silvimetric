import numpy as np
import signal
from datetime import datetime
import copy
import itertools

from typing_extensions import Generator
import pandas as pd

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
    """
    Final method for shatter, add last config attributes and save it to metadata

    :param config: :class:`silvimetric.resources.config.ShatterConfig`.
    :param storage: :class:`silvimetric.resources.storage.Storage`.
    :param finished: Shatter finish flag, defaults to False
    """
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

    :param data_in: Input DataFrame of point data.
    :param metrics: List of Metrics to run.
    :return: DataFrame of derived data.
    """
    graph = Graph(metrics)

    return graph.run(data_in)


def agg_list(data_in: pd.DataFrame, proc_num: int) -> pd.DataFrame:
    """
    Make variable-length point data attributes into lists

    :param data_in: Input DataFrame of point data.
    :param proc_num: Shatter process increment.
    :return: DataFrame of aggregated point data.
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

    :param list_data: DataFrame from agg_list.
    :param metric_data: DataFrame from run_graph.
    :return: Joined DataFrame of Metrics and Attributes.
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
    :param storage: :class:`silvimetric.resources.storage.Storage`.
    :param dates: Tuple of start and end datetimes for dataset.
    :return: Number of points written.
    """
    if data_in.empty:
        return 0

    storage.write(data_in, dates)

    pc = data_in['count'].sum().item()
    p = copy.deepcopy(pc)

    return p


def do_one(leaf: Extents, config: ShatterConfig, storage: Storage) -> pd.DataFrame:
    """
    Create dask bags and the order of operations.

    :param leaf: Extents to operate on.
    :param config: :class:`silvimetric.resources.config.ShatterConfig`.
    :param storage: :class:`silvimetric.resources.storage.Storage`.
    :return: Number of points written.
    """

    # remove any extents that have already been done, only skip if full overlap
    if config.mbr:
        if not all(leaf.disjoint_by_mbr(m) for m in config.mbr):
            return None
    points = get_data(leaf, config.filename, storage)
    if points.empty:
        return None
    listed_data = agg_list(points, config.time_slot)
    metric_data = run_graph(points, storage.get_metrics())
    joined_data = join(listed_data, metric_data)

    del points, listed_data, metric_data

    return joined_data


Leaves = Generator[Extents, None, None]


def run(leaves: Leaves, config: ShatterConfig, storage: Storage) -> int:
    """
    Coordinate running of shatter process and handle any interruptions

    :param leaves: Generator of Leaf nodes.
    :param config: :class:`silvimetric.resources.config.ShatterConfig`
    :param storage: :class:`silvimetric.resources.storage.Storage`
    :return: Number of points processed.
    """

    start_time = int(datetime.now().timestamp()*1000)
    dc = get_client()

    joined_dfs = []
    failures = []

    if dc is not None:
        futures = [dc.submit(do_one, leaf=leaf, config=config, storage=storage) for leaf in leaves]
        res = as_completed(futures, with_results=True, raise_errors=False)
        for future, df in res:
            if future.status == 'error':
                failures.append(df)
                continue

            if df is not None:
                joined_dfs.append(df)

        # TODO write out errors to errors storage path?

            del df
    else:
        processes = [delayed(do_one)(leaf, config, storage) for leaf in leaves]
        results = compute(*processes)

        joined_dfs = [df for df in results if df is not None]
    
    if joined_dfs:
        final_df = pd.concat(joined_dfs, ignore_index=True)
        pc = write(final_df, storage, config.date)
        config.point_count = config.point_count + pc

        del final_df, joined_dfs
    else:
        config.point_count = 0

    end_time = int(datetime.now().timestamp()*1000)
    storage.consolidate(timestamp=(start_time, end_time))

    return config.point_count


def shatter(config: ShatterConfig) -> int:
    """
    Handle setup and running of shatter process.
    Will look for a config that has already been run before and needs to be
    resumed.

    :param config: :class:`silvimetric.resources.config.ShatterConfig`.
    :return: Number of points processed.
    """

    # get start time in milliseconds if not already set
    if config.start_timestamp is None:
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

    leaf_size = storage.config.ysize * storage.config.xsize
    root_ext = Extents(
        bounds=extents.root,
        resolution=extents.resolution,
        alignment=extents.alignment,
        root=extents.root,
    )
    # get leaves reflecting TileDB tile bounds
    potential_leaves = root_ext.get_leaf_children(leaf_size)
    # filter by tiles that overlap, and get the overlapping extent
    filtered_leaves = [
        leaf
        for leaf in potential_leaves if not extents.disjoint(leaf)
    ]
    tiled_leaves = [extents.get_overlap(leaf) for leaf in filtered_leaves]
    full_count = len(tiled_leaves)
    count = 0
    for e in tiled_leaves:
        count = count + 1
        if config.tile_size is not None:
            leaves = e.get_leaf_children(config.tile_size)
        else:
            leaves = e.chunk(data, pc_threshold=config.tile_point_count)

        config.log.debug(f'Shattering extent #{count}/{full_count}.')
        try:
            run(leaves, config, storage)
            storage.vacuum()
            for mode in ['fragment_meta', 'commits', 'array_meta']:
                storage.consolidate(mode)
                storage.vacuum(mode)
        except Exception as e:
            final(config, storage)
            raise e
    storage.consolidate(timestamp=config.timestamp)
    storage.vacuum()

    final(config, storage, finished=True)
    return config.point_count
