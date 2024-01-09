import numpy as np

import dask
import dask.array as da
import dask.bag as db

from ..resources import Extents, Storage, ShatterConfig, Data

def get_data(extents: Extents, filename: str, storage: Storage):
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
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in leaf.get_indices()]
    return dask.persist(*l)

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
    dx = leaf.get_indices()['x']
    dy = leaf.get_indices()['y']
    if bool(empties.size):
        for att in dd:
            dd[att] = np.delete(dd[att], empties)
        dx = np.delete(dx, empties)
        dy = np.delete(dy, empties)
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

    return pc

def run(leaves: db.Bag, config: ShatterConfig, storage: Storage):
    attrs = [a.name for a in config.attrs]

    with storage.open('w') as tdb:

        leaf_bag: db.Bag = db.from_sequence(leaves)
        points: db.Bag = leaf_bag.map(get_data, config.filename, storage)
        att_data: db.Bag = points.map(get_atts, leaf_bag, attrs)
        arranged: db.Bag = att_data.map(arrange, leaf_bag, attrs)
        metrics: db.Bag = arranged.map(get_metrics, attrs, storage)
        writes: db.Bag = metrics.map(write, tdb)
        return sum(writes)



def shatter(config: ShatterConfig):

    config.log.debug('Filtering out empty chunks...')

    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    extents = Extents.from_sub(config.tdb_dir, config.bounds)

    data = Data(config.filename, storage.config, extents.bounds)
    leaves = extents.chunk(data, 100)

    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    config.point_count = int(pc)

    config.log.debug('Saving shatter metadata')
    storage.saveMetadata('shatter', str(config))
    return config.point_count
