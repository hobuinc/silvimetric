import time
from os import path

import tiledb
import pdal
import numpy as np

import dask
import dask.array as da
from dask.distributed import performance_report, progress, Client
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, ProgressBar, visualize

from .bounds import Bounds, Chunk, create_bounds

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def floor_x(points: da.Array, bounds: Bounds):
    return da.array(da.floor((points - bounds.minx) / bounds.cell_size), np.int32)

def floor_y(points: da.Array, bounds: Bounds):
    return da.array(da.floor((points - bounds.miny) / bounds.cell_size), np.int32)

def xform(src_srs: str, dst_srs: str, points: da.Array, bounds: Bounds):
    if src_srs != dst_srs:
        return da.array(bounds.transform(points['X'], points['Y']))
    else:
        return da.array([points['X'], points['Y']])

def get_zs(points: da.Array, chunk: Chunk, bounds: Bounds):
    src_srs = bounds.src_srs
    dst_srs = chunk.srs

    # xform_points = xform(src_srs, dst_srs, points, bounds)
    dt = {
        'names': ['X', 'Y'],
        'formats': ['<f8', '<f8'],
        'offsets': [0, 8],
        'itemsize': 54
    }
    xypoints = points[['X','Y']].view(dt)
    xis = floor_x(xypoints['X'], bounds)
    yis = floor_y(xypoints['Y'], bounds)

    # Set up data object
    zs = dask.compute([
        da.array(points['Z'][cell_indices(xis, yis, x, y)], np.float64)
        for x,y in chunk.indices
    ], scheduler="threads")[0]
    return np.array([*[z for z in zs], None], object)[:-1]

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)
    # remember that readers.copc is a thread hog
    reader._options['threads'] = 2
    pipeline = reader.pipeline()
    pipeline.execute()
    return da.array(pipeline.arrays[0])

def write_tdb(tdb, res):
    dx, dy, dd = res
    tdb[dx, dy] = dd
    return dd['count']

@dask.delayed
def arrange_data(reader, chunk:Chunk, tdb=None):
    points = get_data(reader, chunk)
    bounds = chunk.parent_bounds

    zs = get_zs(points, chunk, bounds)
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': zs }

    dx = chunk.indices['x']
    dy = chunk.indices['y']

    return write_tdb(tdb, [ dx, dy, dd ])

def shatter(filename: str, group_size: int, res: float, local: bool, client: Client,
             polygon=None, p_srs=None, watch=False):

    if 'ept.json' in filename:
        tdbname = path.dirname(filename)
    elif '.copc.' in filename:
        tdbname = path.splitext(path.splitext(path.basename(filename))[0])[0]
    else:
        tdbname = path.splitext(path.basename(filename))[0]

    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

    # set up tiledb
    config = create_tiledb(bounds, tdbname)

    start = time.perf_counter_ns()

    l = []
    with tiledb.open(tdbname, "w", config=config) as tdb:
        if local:
            t = tdb
        else:
            t = client.scatter(tdb)

        for ch in bounds.chunk():
            l.append(arrange_data(reader, ch, t))

        if local:
            with ProgressBar(), Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
                prof.register()
                rprof.register()
                cprof.register()
                futures = dask.compute(l, traverse=True, optimize_graph=True)[0]

        else:
            futures = []
            if watch:
                futures = client.compute(l, optimize_graph=True)
                progress(futures)
                input("Press 'enter' to finish watching.")
            else:
                with performance_report('dask-report.html'):
                    futures = client.compute(l, optimize_graph=True)
                    progress(futures)

        end = time.perf_counter_ns()
        print("Time", (end-start)/(pow(10,9)))

        npcs = da.array([x.flatten() for x in client.gather(futures)]).flatten().compute()

        # print("\nCell point count stats: ")
        # print('  Total:', np.sum(npcs))
        # print('  mean:', np.mean(npcs))
        # print('  median:', np.median(npcs))
        # print('  max:', np.max(npcs))
        # print('  min:', np.min(npcs))

def create_tiledb(bounds: Bounds, dirname):
    if tiledb.object_type(dirname) == "array":
        with tiledb.open(dirname, "d") as A:
            A.query(cond="X>=0").submit()
    else:
        dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        z_att = tiledb.Attr(name="Z", dtype=np.float64, var=True, fill=float(0))

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=1000000, attrs=[count_att, z_att], allows_duplicates=True)
        schema.check()
        tiledb.SparseArray.create(dirname, schema)
    return tiledb.Config({
        "sm.check_coord_oob": False,
        "sm.check_global_order": False,
        "sm.check_coord_dedups": False,
        "sm.dedup_coords": False

    })