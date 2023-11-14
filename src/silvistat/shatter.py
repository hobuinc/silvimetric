import tiledb
import pdal
import numpy as np

from copy import deepcopy
import dask
import dask.array as da
from dask.distributed import performance_report, progress

from .bounds import Bounds, create_bounds
from .storage import Storage

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def floor_x(points: da.Array, bounds: Bounds):
    return da.array(da.floor((points - bounds.minx) / bounds.cell_size),
        np.int32)

def floor_y(points: da.Array, bounds: Bounds):
    return da.array(da.floor((bounds.maxy - points) / bounds.cell_size),
        np.int32)

#TODO move pruning of attributes to this method so we're not grabbing everything
def get_atts(points, chunk):
    bounds = chunk.root
    xypoints = points[['X','Y']].view()
    xis = floor_x(xypoints['X'], bounds)
    yis = floor_y(xypoints['Y'], bounds)
    for xx in xis.compute():
        if xx not in chunk.indices['x']:
            print('x nope')

    for yy in yis.compute():
        if yy not in chunk.indices['y']:
            print('y nope')

    att_data = [da.array(points[:][cell_indices(xis,
        yis, x, y)], dtype=points.dtype) for x,y in chunk.indices]
    asdf = dask.compute(*att_data, scheduler="Threads")
    return asdf

def get_data(pipeline, chunk):
    for stage in pipeline.stages:
        if 'readers' in stage.type:
            reader = stage
            break
    # reader._options['bounds'] = str(chunk)
    pipeline = pipeline | pdal.Filter.crop(bounds=str(chunk))

    try:
        pipeline.execute()
    except Exception as e:
        print(pipeline.pipeline, e)

    return da.array(pipeline.arrays[0])

@dask.delayed
def arrange_data(pipeline, chunk: Bounds, atts, storage=None):

    points = get_data(deepcopy(pipeline), chunk)
    if not points.size:
        return 0

    data = get_atts(points, chunk)

    dd = {}
    for att in atts:
        try:
            dd[att] = np.array([col[att] for col in data], object)
        except Exception as e:
            raise(f"Missing attribute {att}: {e}")

    counts = np.array([z.size for z in dd['Z']], np.int32)
    # if this trips, something is wrong with the the matchup between indices
    # and bounds of the chunk
    assert points.size == counts.sum(), "\
        Data read size doesn't match attribute counts"

    ## remove empty indices and create final sparse tiledb inputs
    empties = np.where(counts == 0)
    dd['count'] = counts
    for att in dd:
        dd[att] = np.delete(dd[att], empties)
    dx = np.delete(chunk.indices['x'], empties)
    dy = np.delete(chunk.indices['y'], empties)

    if storage != None:
        storage.write(dx, dy, dd)
    sum = counts.sum()
    # del data, dd, points, chunk
    return sum

def create_pipeline(filename):
    reader = pdal.Reader(filename, tag='reader')
    reader._options['threads'] = 2
    class_zero = pdal.Filter.assign(value="Classification = 0")
    rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
    nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
    # smrf = pdal.Filter.smrf()
    # hag = pdal.Filter.hag_nn()
    return reader | class_zero | rn | nor #| smrf | hag

def run(pipeline, bounds, tdb_config, filter_res, tdb_dir, atts, client, debug):
    with tiledb.open(tdb_dir, "w", config=tdb_config) as tdb:
        # debug uses single threaded dask
        if debug:
            leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in
                get_leaves(filter_res)])[0]
            data_futures = dask.compute([arrange_data(pipeline, ch, bounds, atts, tdb) for leaf in leaf_procs
                for ch in leaf], optimize_graph=True)
        else:
            with performance_report(f'{tdb_dir}-dask-report.html'):
                t = client.scatter(tdb)
                b = client.scatter(bounds)

                leaf_procs = client.compute([node.get_leaf_children() for node
                    in get_leaves(filter_res)])

                data_futures = client.compute([
                    arrange_data(pipeline, ch, b, atts, t)
                    for leaf in leaf_procs for ch in leaf
                ])

                progress(data_futures)
                client.gather(data_futures)
        c = 0
        for f in data_futures:
            c += sum(f)
        return c

def shatter(filename: str, tdb_dir: str, group_size: int, res: float,
            debug: bool, client=None, polygon=None, atts=['Z']):
    # read pointcloud
    pipeline = create_pipeline(filename)
    reader = pipeline.stages[0]
    bounds = create_bounds(reader, res, group_size, polygon)
    print('Filtering out empty chunks...')

    # set up tiledb
    storage = Storage(tdb_dir, filename, atts)
    config = storage.init(bounds, tdb_dir, atts)

    # Begin main operations
    print('Fetching and arranging data...')
    f = bounds.chunk(filename)
    run(pipeline, bounds, config, f, tdb_dir, atts, client, debug)
