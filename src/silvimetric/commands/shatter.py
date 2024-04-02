import numpy as np
import copy
import signal
import datetime

import dask
from dask.distributed import as_completed, futures_of, CancelledError
import dask.array as da
import dask.bag as db

from ..resources import Extents, Storage, ShatterConfig, Data

def get_data(extents: Extents, filename: str, storage: Storage):
    #TODO look at making a record array here
    data = Data(filename, storage.config, bounds = extents.bounds)
    data.execute()
    return data.array

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def get_atts(points: np.ndarray, leaf: Extents, attrs: list[str]):
    if points.size == 0:
        return None

    xis = da.floor(points[['xi']]['xi'])
    yis = da.floor(points[['yi']]['yi'])

    att_view = points[:][attrs]
    idx = leaf.get_indices()
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in idx]
    return l

def arrange(data: tuple[np.ndarray, np.ndarray, np.ndarray], leaf: Extents, attrs):
    if data is None:
        return None

    di = data
    dd = {}
    for att in attrs:
        try:
            dd[att] = np.fromiter([*[np.array(col[att], col[att].dtype) for col in di], None], dtype=object)[:-1]
        except Exception as e:
            raise Exception(f"Missing attribute {att}: {e}")
    counts = np.array([z.size for z in dd['Z']], np.int32)

    ## remove empty indices
    empties = np.where(counts == 0)[0]
    dd['count'] = counts
    idx = leaf.get_indices()
    if bool(empties.size):
        for att in dd:
            dd[att] = np.delete(dd[att], empties)
        idx = np.delete(idx, empties)

    dx = idx['x']
    dy = idx['y']
    return (dx, dy, dd)

def get_metrics(data_in, attrs: list[str], storage: Storage):
    if data_in is None:
        return None

    ## data comes in as [dx, dy, { 'att': [data] }]
    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return None

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    metric_data = {
        f'{m.entry_name(attr)}': [m(cell_data) for cell_data in data[attr]]
        for attr in attrs for m in storage.config.metrics
    }
    data_out = data | metric_data
    return (dx, dy, data_out)

def write(data_in, tdb):

    if data_in is None:
        return 0

    dx, dy, dd = data_in
    tdb[dx,dy] = dd
    pc = int(dd['count'].sum())
    p = copy.deepcopy(pc)
    del pc, data_in

    return p

def run(leaves: db.Bag, config: ShatterConfig, storage: Storage):
    ## Handle dask bag transitions through work states
    attrs = [a.name for a in config.attrs]
    timestamp = (config.time_slot, config.time_slot)

    with storage.open('w', timestamp=timestamp) as tdb:
        leaf_bag: db.Bag = db.from_sequence(leaves)
        points: db.Bag = leaf_bag.map(get_data, config.filename, storage)
        att_data: db.Bag = points.map(get_atts, leaf_bag, attrs)
        arranged: db.Bag = att_data.map(arrange, leaf_bag, attrs)
        metrics: db.Bag = arranged.map(get_metrics, attrs, storage)
        writes: db.Bag = metrics.map(write, tdb)

        ## If dask is distributed, use the futures feature
        if dask.config.get('scheduler') == 'distributed':
            pc_futures = futures_of(writes.persist())
            for batch in as_completed(pc_futures, with_results=True).batches():
                for future, pack in batch:
                    if isinstance(pack, CancelledError):
                        continue
                    for pc in pack:
                        config.point_count = config.point_count + pc

            end_time = datetime.datetime.now().timestamp() * 1000
            config.end_time = end_time
            config.finished = True
            return config.point_count

        ## Handle non-distributed dask scenarios
        pc = sum(writes)

    # modify config to reflect result of shattter process
    with storage.open('r', timestamp=timestamp) as a:
        config.nonempty_domain = a.nonempty_domain()
    config.log.debug('Saving shatter metadata')
    config.end_time = datetime.datetime.now().timestamp() * 1000
    config.finished = True
    config.point_count = pc

    storage.saveMetadata('shatter', str(config), config.time_slot)
    return pc


def shatter(config: ShatterConfig):
    # get start time in milliseconds
    config.start_time = datetime.datetime.now().timestamp() * 1000
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    data = Data(config.filename, storage.config, config.bounds)
    extents = Extents.from_sub(config.tdb_dir, data.bounds)

    if not config.time_slot: # defaults to 0, which is reserved for storage cfg
        config.time_slot = storage.reserve_time_slot()

    # Process kill handler. Make sure we write out a config even if we fail.
    def kill_gracefully(signum, frame):
        client = dask.config.get('distributed.client')
        if client is not None:
            client.close()
        end_time = datetime.datetime.now().timestamp() * 1000
        config.end_time = end_time

        with storage.open('r', timestamp=(config.time_slot, config.time_slot)) as a:
            config.nonempty_domain = a.nonempty_domain()
            config.finished=False

            config.log.info('Saving config before quitting...')
            storage.saveMetadata('shatter', str(config), config.time_slot)
            config.log.info('Quitting.')

    signal.signal(signal.SIGINT, kill_gracefully)

    if config.bounds is None:
        config.bounds = extents.bounds

    config.log.debug('Grabbing leaf nodes...')
    if config.tile_size is not None:
        leaves = extents.get_leaf_children(config.tile_size)
    else:
        leaves = extents.chunk(data, 100)

    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    return run(leaves, config, storage)