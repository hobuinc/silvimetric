import pdal
import numpy as np
from line_profiler import profile
import itertools

import dask
import dask.array as da
import dask.bag as db
from dask.distributed import performance_report, Client

from ..resources import Bounds, Extents, Storage, Metric, ShatterConfig

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def floor_x(points: da.Array, bounds: Bounds, resolution: float):
    return da.array(da.floor((points - bounds.minx) / resolution),
        np.int32)

def floor_y(points: da.Array, bounds: Bounds, resolution: float):
    return da.array(da.floor((bounds.maxy - points) / resolution),
        np.int32)

@dask.delayed
@profile
def get_atts(points: da.Array, chunk: Extents, attrs: list[str]):
    xis = da.floor(points[['xi']]['xi'])
    yis = da.floor(points[['yi']]['yi'])

    att_view = points[:][attrs]
    # l = []
    # for x, y in chunk.indices:
    #     indices = cell_indices(xis, yis, x, y)
    #     element = dask.delayed(lambda a,i: a[i])(att_view, indices)
    #     l.append(element)
    # return dask.compute(*l, scheduler="threads")
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in chunk.indices]
    return dask.compute(*l, scheduler="threads")

@dask.delayed
@profile
def get_metrics(data_in, attrs: list[str], metrics: list[Metric],
                storage: Storage):
    ## data comes in as [dx, dy, { 'att': [data] }]
    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return 0

    for attr in attrs:
        for m in metrics:
            m: Metric
            name = m.entry_name(attr)
            data[name] = np.array([m(cell_data) for cell_data in data[attr]],
                                  m.dtype).flatten(order='C')

    storage.write(dx,dy,data)
    return data['count'].sum()

@dask.delayed
@profile
def get_data(filename, chunk):
    pipeline = create_pipeline(filename, chunk)
    try:
        pipeline.execute()
    except Exception as e:
        print(pipeline.pipeline, e)

    return pipeline.arrays[0]

@dask.delayed
@profile
def arrange(chunk, data, attrs):
    dd = {}
    for att in attrs:
        try:
            dd[att] = np.fromiter([*[col[att] for col in data], None], dtype=object)[:-1]
        except Exception as e:
            raise Exception(f"Missing attribute {att}: {e}")
    counts = np.array([z.size for z in dd['Z']], np.int32)

    ## remove empty indices
    empties = np.where(counts == 0)[0]
    dd['count'] = counts
    dx = chunk.indices['x']
    dy = chunk.indices['y']
    if bool(empties.size):
        for att in dd:
            dd[att] = np.delete(dd[att], empties)
        dx = np.delete(dx, empties)
        dy = np.delete(dy, empties)
    return [dx, dy, dd]


def create_pipeline(filename, chunk):
    reader = pdal.Reader(filename, tag='reader')
    reader._options['threads'] = 2
    reader._options['bounds'] = str(chunk)
    class_zero = pdal.Filter.assign(value="Classification = 0")
    rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
    nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
    ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")
    assign_x = pdal.Filter.assign(
        value=f"xi = (X - {chunk.root.minx}) / {chunk.resolution}")
    assign_y = pdal.Filter.assign(
        value=f"yi = ({chunk.root.maxy} - Y) / {chunk.resolution}")
    # smrf = pdal.Filter.smrf()
    # hag = pdal.Filter.hag_nn()
    # return reader | crop | class_zero | rn | nor #| smrf | hag
    return reader | class_zero | rn | nor | ferry | assign_x | assign_y #| smrf | hag

@dask.delayed
def one(leaf: Extents, config: ShatterConfig, storage: Storage):
    attrs = [a.name for a in config.attrs]

    points = get_data(config.filename, leaf)
    att_data = get_atts(points, leaf, attrs)
    arranged = arrange(leaf, att_data, attrs)
    m = get_metrics(arranged, attrs, config.metrics, storage)
    return dask.compute(m, scheduler="threads")[0]
    # return dask.compute(m, scheduler="threads")[0]

def run(leaves: db.Bag, config: ShatterConfig, storage: Storage):
    l = []
    if config.debug:
        l = db.map(one, leaves, config, storage)
        vals = dask.compute(*l.compute())
    else:
        with performance_report(f'dask-report-1.html'):
            for leaf in leaves:
                l.append(one(leaf,config,storage))
            vals = dask.compute(*l)

    return sum(vals)

def shatter(config: ShatterConfig, client: Client=None):
    print('Filtering out empty chunks...')
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    extents = Extents.from_storage(storage, config.tile_size)
    leaves = db.from_sequence(extents.chunk(config.filename, 1000))

    # Begin main operations
    print('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    config.point_count = pc.item()
    storage.saveMetadata('shatter', str(config))
