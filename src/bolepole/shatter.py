import time

import tiledb
import pdal
import numpy as np

import dask
import dask.array as da
from dask.distributed import performance_report, progress, Client
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, ProgressBar, visualize

from .bounds import Bounds, Chunk, create_bounds

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(
        xpoints.astype(np.int32) == x,
        ypoints.astype(np.int32) == y
    )

def floor_x(points: da.Array, bounds: Bounds):
    return da.floor((points - bounds.minx) / bounds.cell_size)

def floor_y(points: da.Array, bounds: Bounds):
    return da.floor((points - bounds.miny) / bounds.cell_size)

def xform(src_srs: str, dst_srs: str, points: da.Array, bounds: Bounds):
    if src_srs != dst_srs:
        return da.array(bounds.transform(points['X'], points['Y']))
    else:
        return da.array([points['X'], points['Y']])

def where_true(points, idx):
    return da.array(points['Z'][da.where(idx==True)], np.float64)

def get_zs(points, chunk, bounds):
    src_srs = bounds.src_srs
    dst_srs = chunk.srs

    # xform_points = xform(src_srs, dst_srs, points, bounds)
    xis = floor_x(points['X'], bounds)
    yis = floor_y(points['Y'], bounds)

    # Set up data object
    zs = []
    for x, y in chunk.indices:
        idx = cell_indices(xis, yis, x, y)
        zs.append(where_true(points, idx))

    new_zs = dask.compute(zs, scheduler="threads")[0]
    ret = np.array([*[z for z in new_zs], None], object)[:-1]
    return ret

def arrange_data(points: da.Array, chunk:Chunk, tdb=None):
    bounds = chunk.parent_bounds

    zs = get_zs(points, chunk, bounds)
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': zs }

    dx = np.array([], np.int32)
    dy = np.array([], np.int32)
    for x, y in chunk.indices:
        dx = np.append(dx, x)
        dy = np.append(dy, y)

    return write_tdb(tdb, [ dx, dy, dd ])
    return [ dx, dy, dd ]

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)
    # remember that readers.copc is a thread hog
    reader._options['threads'] = 2
    pipeline = reader.pipeline()
    pipeline.execute()
    return da.array(pipeline.arrays[0])

def write_tdb(tdb, res):
    dx, dy, dd = res
    # dd = dask.persist({k: v.astype(np.dtype(v.dtype.kind)) for k,v in dd.items()})
    tdb[dx, dy] = dd
    return dd['count']

def run_one(reader, chunk: Chunk, tdb=None):
    point_data = dask.delayed(get_data)(reader=reader, chunk=chunk)
    arranged_data = dask.delayed(arrange_data)(points=point_data, chunk=chunk, tdb=tdb)
    return arranged_data
    return dask.delayed(write_tdb)(tdb, arranged_data)

def shatter(filename: str, group_size: int, res: float, local: bool, client: Client,
             polygon=None, p_srs=None, watch=False):
    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

    # set up tiledb
    config = create_tiledb(bounds)

    start = time.perf_counter_ns()

    l = []
    with tiledb.open("stats", "w", config=config) as tdb:
        if local:
            t = tdb
        else:
            t = client.scatter(tdb)

        for ch in bounds.chunk():
            l.append(run_one(reader, ch, t))

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
                with performance_report():
                    futures = client.compute(l, optimize_graph=True)
                    progress(futures)

        end = time.perf_counter_ns()
        print("Time", (end-start)/(pow(10,9)))

        dpcs = da.array([], dtype=np.int32)
        for count in client.gather(futures):
            dpcs = da.append(dpcs, count)
        npcs = np.array(dpcs.compute())

        print("\nCell point count stats: ")
        print('  mean:', np.mean(npcs))
        print('  median:', np.median(npcs))
        print('  max:', np.max(npcs))
        print('  min:', np.min(npcs))

def create_tiledb(bounds: Bounds):
    if tiledb.object_type("stats") == "array":
        with tiledb.open("stats", "d") as A:
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
        tiledb.SparseArray.create('stats', schema)
    return tiledb.Config({
        "sm.check_coord_oob": False,
        "sm.check_global_order": False,
        "sm.check_coord_dedups": False,
        "sm.dedup_coords": False

    })