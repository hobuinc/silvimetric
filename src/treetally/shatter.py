import tiledb
import pdal
import numpy as np

from copy import deepcopy
import dask
import dask.array as da
from dask.diagnostics import ProgressBar
from dask.distributed import performance_report, progress, Client

from .bounds import Bounds, create_bounds

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

def write_tdb(tdb, res):
    dx, dy, dd = res
    tdb[dx, dy] = dd

@dask.delayed
def arrange_data(pipeline, chunk: Bounds, atts, tdb=None):

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

    if tdb != None:
        write_tdb(tdb, [ dx, dy, dd ])
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
    config = create_tiledb(bounds, tdb_dir, atts)

    # Begin main operations
    print('Fetching and arranging data...')
    f = bounds.chunk(filename)
    run(pipeline, bounds, config, f, tdb_dir, atts, client, debug)

def create_tiledb(bounds: Bounds, dirname: str, atts: list[str]):
    dims = { d['name']: d['dtype'] for d in pdal.dimensions }
    if tiledb.object_type(dirname) == "array":
        with tiledb.open(dirname, "d") as A:
            A.query(cond="X>=0").submit()
    else:
        dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True) for name in atts]

        # z_att = tiledb.Attr(name="Z", dtype=np.float64, var=True)
        # hag_att = tiledb.Attr(name="HeightAboveGround", dtype=np.float32, var=True)
        # atts = [count_att, z_att, hag_att]

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=len(atts)*bounds.xi*bounds.yi, attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()
        tiledb.SparseArray.create(dirname, schema)

    return tiledb.Config({
        "sm.check_coord_oob": False,
        "sm.check_global_order": False,
        "sm.check_coord_dedups": False,
        "sm.dedup_coords": False
    })