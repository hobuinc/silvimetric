import numpy as np
import signal
import datetime
import copy
from typing import Generator
import pandas as pd

import dask
from dask.distributed import as_completed, futures_of, CancelledError
import dask.array as da
import dask.bag as db
from line_profiler import profile

from .. import Extents, Storage, Data, ShatterConfig

@profile
def get_data(extents: Extents, filename: str, storage: Storage) -> np.ndarray:
    """
    Execute pipeline and retrieve point cloud data for this extent

    :param extents: :class:`silvimetric.resources.extents.Extents` being operated on.
    :param filename: Path to either PDAL pipeline or point cloud.
    :param storage: :class:`silvimetric.resources.storage.Storage` database object.
    :return: Point data array from PDAL.
    """
    #TODO look at making a record array here
    data = Data(filename, storage.config, bounds = extents.bounds)
    p = data.pipeline
    data.execute()
    return p.get_dataframe(0)

@profile
def arrange(points: pd.DataFrame, leaf, attrs: list[str]):
    """
    Arrange data to fit key-value TileDB input format.

    :param data: Tuple of indices and point data array (xis, yis, data).
    :param leaf: :class:`silvimetric.resources.extents.Extent` being operated on.
    :param attrs: List of attribute names.
    :raises Exception: Missing attribute error.
    :return: None if no work is done, or a tuple of indices and rearranged data.
    """
    if points is None:
        return None
    if points.size == 0:
        return None

    points = points.loc[points.Y < leaf.bounds.maxy]
    points = points.loc[points.X < leaf.bounds.maxx, [*attrs, 'xi', 'yi']]

    points.loc[:, 'xi'] = da.floor(points.xi)
    points.loc[:, 'yi'] = da.floor(points.yi)

    return points


@profile
def get_metrics(data_in, storage: Storage):
    if data_in is None:
        return None

    metric_data = dask.persist(*[ m.do(data_in) for m in storage.config.metrics ])
    return metric_data

@profile
def agg_list(df: pd.DataFrame):
    if df is None:
        return None
    grouped = df.groupby(['xi','yi'])
    grouped = grouped.agg(list)
    return grouped.assign(count=lambda x: [len(z) for z in grouped.Z])

@profile
def join(list_data, metric_data):
    if list_data is None or metric_data is None:
        return None
    return list_data.join([m for m in metric_data])

@profile
def write(data_in, tdb):
    """
    Write cell data to database

    :param data_in: Data to be written to database.
    :param tdb: TileDB write stream.
    :return: Number of points written.
    """
    if data_in is None:
        return 0

    # TODO get this working at some point. Look at pandas extensions
    # data_in = data_in.rename(columns={'xi':'X','yi':'Y'})

    # tiledb.from_pandas(uri='autzen_db', dataframe=data_in, mode='append',
    #         column_types=dict(data_in.dtypes),
    #         varlen_types=[np.dtype('O')])

    idf = data_in.index.to_frame()

    dx = idf['xi'].to_list()
    dy = idf['yi'].to_list()
    dt = lambda a: tdb.schema.attr(a).dtype
    dd = { d: np.fromiter([*[np.array(nd, dt(d)) for nd in data_in[d]], None], object)[:-1] for d in data_in }

    tdb[dx,dy] = dd
    pc = dd['count'].sum().item()
    p = copy.deepcopy(pc)
    del pc, data_in
    return p

Leaves = Generator[Extents, None, None]
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
        client = dask.config.get('distributed.client')
        if client is not None:
            client.close()
        end_time = datetime.datetime.now().timestamp() * 1000

        storage.consolidate_shatter(config.time_slot)
        config.end_time = end_time
        config.mbrs = storage.mbrs(config.time_slot)
        config.finished=False
        config.log.info('Saving config before quitting...')

        storage.saveMetadata('shatter', str(config), config.time_slot)
        config.log.info('Quitting.')

    signal.signal(signal.SIGINT, kill_gracefully)


    ## Handle dask bag transitions through work states
    attrs = [a.name for a in config.attrs]
    timestamp = (config.time_slot, config.time_slot)

    with storage.open('w', timestamp=timestamp) as tdb:
        leaf_bag: db.Bag = db.from_sequence(leaves)
        if config.mbr:
            def mbr_filter(one: Extents):
                return all(one.disjoint_by_mbr(m) for m in config.mbr)
            leaf_bag = leaf_bag.filter(mbr_filter)

        points: db.Bag = leaf_bag.map(get_data, config.filename, storage)
        arranged: db.Bag = points.map(arrange, leaf_bag, attrs)
        metrics: db.Bag = arranged.map(get_metrics, storage)
        lists: db.Bag = arranged.map(agg_list)
        joined: db.Bag = lists.map(join, metrics)
        writes: db.Bag = joined.map(write, tdb)

        ## If dask is distributed, use the futures feature
        dc = dask.config.get('distributed.client')
        if isinstance(dc, dask.distributed.Client):
            pc_futures = futures_of(writes.persist())
            for batch in as_completed(pc_futures, with_results=True).batches():
                for future, pack in batch:
                    if isinstance(pack, CancelledError):
                        continue
                    for pc in pack:
                        config.point_count = config.point_count + pc
                        del pc

            end_time = datetime.datetime.now().timestamp() * 1000
            config.end_time = end_time
            config.finished = True

        ## Handle non-distributed dask scenarios
        else:
            config.point_count = sum(writes)

    # modify config to reflect result of shattter process
    config.mbr = storage.mbrs(config.time_slot)
    config.log.debug('Saving shatter metadata')
    config.end_time = datetime.datetime.now().timestamp() * 1000
    config.finished = True

    storage.saveMetadata('shatter', str(config), config.time_slot)
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

    if dask.config.get('distributed.client') is None:
        config.log.warning("Selected scheduler type does not support"
                "continuously updated config information.")

    if not config.time_slot: # defaults to 0, which is reserved for storage cfg
        config.time_slot = storage.reserve_time_slot()

    if config.bounds is None:
        config.bounds = extents.bounds

    config.log.debug('Grabbing leaf nodes...')
    if config.tile_size is not None:
        leaves = extents.get_leaf_children(config.tile_size)
    else:
        leaves = extents.chunk(data, 100)

    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    storage.consolidate_shatter(config.time_slot)
    return pc