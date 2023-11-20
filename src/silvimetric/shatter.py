import tiledb
import pdal
import numpy as np

from copy import deepcopy
import dask
import dask.array as da
from dask.distributed import performance_report, progress

from .bounds import Bounds
from .extents import Extents
from .storage import Storage

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def floor_x(points: da.Array, bounds: Bounds, resolution: float):
    return da.array(da.floor((points - bounds.minx) / resolution),
        np.int32)

def floor_y(points: da.Array, bounds: Bounds, resolution: float):
    return da.array(da.floor((bounds.maxy - points) / resolution),
        np.int32)

#TODO move pruning of attributes to this method so we're not grabbing everything
def get_atts(points, chunk):
    bounds = chunk.root
    xypoints = points[['X','Y']].view()
    xis = floor_x(xypoints['X'], bounds, chunk.resolution)
    yis = floor_y(xypoints['Y'], bounds, chunk.resolution)
    att_data = [da.array(points[:][cell_indices(xis,
        yis, x, y)], dtype=points.dtype) for x,y in chunk.indices]
    return dask.compute(*att_data, scheduler="Threads")
    # return dask.compute(*att_data)

def get_data(filename, chunk):
    # for stage in pipeline.stages:
    #     if 'readers' in stage.type:
    #         reader = stage
    #         break
    # reader._options['bounds'] = str(chunk)
    # pipeline = pipeline |
    pipeline = create_pipeline(filename, chunk)
    try:
        pipeline.execute()
    except Exception as e:
        print(pipeline.pipeline, e)

    return da.array(pipeline.arrays[0])

@dask.delayed
def arrange_data(filename, chunk: Extents, atts: list[str],
                 tdb: tiledb.SparseArray=None):

    points = get_data(filename, chunk)
    if not points.size:
        return 0

    data = get_atts(points, chunk)

    dd = {}
    for att in atts:
        try:
            dd[att] = np.array([*np.array([col[att] for col in data], object), None], object)[:-1]
        except Exception as e:
            raise Exception(f"Missing attribute {att}: {e}")

    counts = np.array([z.size for z in dd['Z']], np.int32)

    ## remove empty indices and create final sparse tiledb inputs
    empties = np.where(counts == 0)
    dd['count'] = counts
    for att in dd:
        dd[att] = np.delete(dd[att], empties)
    dx = np.delete(chunk.indices['x'], empties)
    dy = np.delete(chunk.indices['y'], empties)

    if tdb is not None:
        print(dx)
        print(dy)
        print(dd)
        tdb[dx, dy] = dd
    sum = counts.sum()
    del data, dd, points, chunk
    return sum

def create_pipeline(filename, chunk):
    reader = pdal.Reader(filename, tag='reader')
    reader._options['threads'] = 2
    crop = pdal.Filter.crop(bounds=str(chunk))
    class_zero = pdal.Filter.assign(value="Classification = 0")
    rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
    nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
    # smrf = pdal.Filter.smrf(
    # hag = pdal.Filter.hag_nn()
    return reader | crop | class_zero | rn | nor #| smrf | hag

def run(filename, atts, leaves, tdb: tiledb.SparseArray, client, debug):
    # debug uses single threaded dask
    if debug:
        data_futures = dask.compute([arrange_data(filename, leaf, atts, tdb) for leaf in leaves])
    else:
        with performance_report(f'dask-report.html'):
            data_futures = dask.compute([arrange_data(filename, leaf, atts, tdb) for leaf in leaves])

            progress(data_futures)
            client.gather(data_futures)
    c = 0
    for f in data_futures:
        c += sum(f)
    return c

#TODO OPTIMIZE: try using dask.persist and seeing if you can break up the tasks
# into things like get_data, get_atts, arrange_data so that they aren't waiting
# on each other to compute and still using the resources.
def shatter(filename: str, tdb_dir: str, tile_size: int, debug: bool=False, client=None):
    print('Filtering out empty chunks...')
    # set up tiledb
    storage = Storage(tdb_dir)
    atts = storage.getAttributes()
    atts.remove('count')
    extents = Extents.from_storage(storage, tile_size)
    leaves = list(extents.chunk(filename, 1000))

    # Begin main operations
    with storage.open('w') as tdb:
        print('Fetching and arranging data...')
        pc = run(filename, atts, leaves, tdb, client, debug)
        storage.saveMetadata({'point_count': pc.item()})
    # storage.consolidate()
