import pdal
import numpy as np
from line_profiler import profile
import itertools

import dask
import dask.array as da
from dask.distributed import performance_report, progress, Client

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
    bounds = chunk.root
    xypoints = points[['X','Y']].view()
    xis = floor_x(xypoints['X'], bounds, chunk.resolution)
    yis = floor_y(xypoints['Y'], bounds, chunk.resolution)
    # get attribute_data
    att_view = points[:][attrs]
    dt = att_view.dtype
    return [np.array(att_view[cell_indices(xis, yis, x, y)], dt)
                for x,y in chunk.indices]

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
            # dd[att] = np.array( dtype=object, object=[ itertools.chain( [ np.array( [ [ col[att] for col in data ], [ None ] ]) ])])
            # dd[att] = np.array(dtype=object, object=[
            #     *[np.array(col[att], data[0][att].dtype) for col in data],
            #     None])[:-1]
        except Exception as e:
            raise Exception(f"Missing attribute {att}: {e}")
    counts = np.array([z.size for z in dd['Z']], np.int32)

    ## remove empty indices
    empties = np.where(counts != 0)[0]
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
    # crop = pdal.Filter.crop(bounds=str(chunk))
    class_zero = pdal.Filter.assign(value="Classification = 0")
    rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
    nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
    # smrf = pdal.Filter.smrf()
    # hag = pdal.Filter.hag_nn()
    # return reader | crop | class_zero | rn | nor #| smrf | hag
    return reader | class_zero | rn | nor #| smrf | hag

@profile
def one(leaf: Extents, config: ShatterConfig, storage: Storage):
    attrs = [a.name for a in config.attrs]

    points = get_data(config.filename, leaf)
    att_data = get_atts(points, leaf, attrs)
    arranged = arrange(leaf, att_data, attrs)
    return get_metrics(arranged, attrs, config.metrics, storage)

def run(leaves: list[Extents], config: ShatterConfig, storage: Storage):
    count = 0
    l = []

    if config.debug:
        for leaf in leaves:
            l.append(one(leaf, config, storage))
            # l.append(write(packed, storage))
        vals = dask.compute(*l)
    else:
        with performance_report(f'dask-report-1.html'):
            l = []
            for leaf in leaves:
                l.append(one(leaf,config,storage))

            vals = dask.compute(*l)

    for pc in vals:
        count += pc

    return count

#TODO OPTIMIZE: try using dask.persist and seeing if you can break up the tasks
# into things like get_data, get_atts, arrange_data so that they aren't waiting
# on each other to compute and still using the resources.
def shatter(config: ShatterConfig, client: Client=None):
    print('Filtering out empty chunks...')
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    extents = Extents.from_storage(storage, config.tile_size)
    leaves = list(extents.chunk(config.filename, 1000))

    # Begin main operations
    print('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    config.point_count = pc
    storage.saveMetadata('shatter', str(config))