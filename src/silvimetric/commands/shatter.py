import numpy as np

import dask
import dask.array as da
import dask.bag as db
from dask.delayed import Delayed

from ..resources import Extents, Storage, Metric, ShatterConfig, Data, StorageConfig, Bounds

def get_data(bounds: Bounds, filename: str, storage: Storage):
    data = Data(filename, storage.config, bounds = bounds)
    data.execute()
    return data.array

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def get_atts(points: da.Array, attrs: list[str]):
    xis = da.floor(points[['xi']]['xi'])
    yis = da.floor(points[['yi']]['yi'])

    indices = np.array(
        [(i,j) for i in range(int(xis.min()), int(xis.max()))
        for j in range(int(yis.min()), int(yis.max()))],
        dtype=[('x', np.int32), ('y', np.int32)]
    )

    att_view = points[:][attrs]
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in indices]
    return (indices['x'], indices['y'], dask.persist(*l))

def arrange(data: tuple[np.ndarray, np.ndarray, np.ndarray], attrs):
    dx, dy, di = data

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
    if bool(empties.size):
        for att in dd:
            dd[att] = np.delete(dd[att], empties)
        dx = np.delete(dx, empties)
        dy = np.delete(dy, empties)
    return [dx, dy, dd]


def get_metrics(data_in, attrs: list[str], storage: Storage):

    ## data comes in as [dx, dy, { 'att': [data] }]
    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return 0

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    metric_data = {
        f'{m.entry_name(attr)}': dask.persist(*[m(cell_data) for cell_data in data[attr]])
        for attr in attrs for m in storage.config.metrics
    }
    full_data = data | metric_data

    storage.write(dx,dy,full_data)
    pc = data['count'].sum()
    return pc

def run(leaves: db.Bag, config: ShatterConfig, storage: Storage):
    attrs = [a.name for a in config.attrs]

    points: db.Bag = leaves.map(get_data, config.filename, storage).persist()
    att_data: db.Bag = points.map(get_atts, attrs).persist()
    arranged: db.Bag = att_data.map(arrange, attrs).persist()
    metrics: db.Bag = arranged.map(get_metrics, attrs, storage)

    vals = metrics.persist()

    return sum(vals)


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
