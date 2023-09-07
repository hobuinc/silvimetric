import time
import math
import argparse
import webbrowser

import tiledb
import pdal
import numpy as np
from shapely import from_wkt
from pyproj import CRS

import dask
import dask.array as da
from dask.distributed import performance_report, progress, LocalCluster, PipInstall, UploadDirectory
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, ProgressBar, visualize

from .bounds import Bounds, Chunk

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
        return bounds.transform(points['X'], points['Y'])
    else:
        return [ points['X'], points['Y'] ]

def where_true(points, idx):
    return np.array(points['Z'][np.where(idx==True)], np.float64)

def get_zs(points, chunk, bounds):
    src_srs = bounds.src_srs
    dst_srs = chunk.srs

    x_points, y_points = xform(src_srs, dst_srs, points, bounds)
    xis = dask.delayed(floor_x)(x_points, bounds)
    yis = dask.delayed(floor_y)(y_points, bounds)

    # Set up data object
    zs = []
    for x, y in chunk.indices:
        idx = dask.delayed(cell_indices)(xis, yis, x, y)
        zs.append(dask.delayed(where_true)(points, idx))

    return np.array([*[z for z in dask.compute(zs, optimize_graph=True)[0]], None], object)[:-1]

def arrange_data(point_data: tuple[da.Array, Chunk], bounds: Bounds):
    points, chunk = point_data

    zs = get_zs(points, chunk, bounds)
    # npz = np.array([*[np.array(z, np.float64) for z in zs], None], dtype=object)[:-1]
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': zs }

    dx = da.array([], np.int32)
    dy = da.array([], np.int32)
    for x, y in chunk.indices:
        dx = da.append(dx, x)
        dy = da.append(dy, y)

    return [ dx, dy, dd ]

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)
    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = da.array(pipeline.arrays[0])
    return [points, chunk]

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
            capacity=10000, attrs=[count_att, z_att], allows_duplicates=True)
        schema.check()
        tiledb.SparseArray.create('stats', schema)

def create_bounds(reader, cell_size, group_size, polygon=None, p_srs=None) -> Bounds:
    # grab our bounds
    if polygon:
        p = from_wkt(polygon)
        if not p.is_valid:
            raise Exception("Invalid polygon entered")

        b = p.bounds
        minx = b[0]
        miny = b[1]
        if len(b) == 4:
            maxx = b[2]
            maxy = b[3]
        elif len(b) == 6:
            maxx = b[3]
            maxy = b[4]
        else:
            raise Exception("Invalid bounds found.")

        # TODO handle srs that's geographic
        user_crs = CRS.from_user_input(p_srs)
        user_wkt = user_crs.to_wkt()

        bounds = Bounds(minx, miny, maxx, maxy, cell_size, group_size, user_wkt)
        bounds.reproject()

        reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()

        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        src_srs = qi['srs']['wkt']
        bounds.set_transform(src_srs)

        if not pc:
            raise Exception("No points found.")

        return bounds
    else:

        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]

        if not qi['num_points']:
            raise Exception("No points found.")

        bbox = qi['bounds']
        minx = bbox['minx']
        maxx = bbox['maxx']
        miny = bbox['miny']
        maxy = bbox['maxy']
        srs = qi['srs']['wkt']
        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                    group_size=group_size, srs=srs)
        bounds.set_transform(srs)

        return bounds

def write_tdb(res):
    with tiledb.SparseArray("stats", "w") as tdb:
        dx, dy, dd = res
        dd = {k: v.astype(np.dtype(v.dtype.kind)) for k,v in dd.items()}
        tdb[dx, dy] = dd
        return dd['count'].sum()

def run_one(reader, chunk: Chunk, local=False):
    point_data = dask.delayed(get_data)(reader=reader, chunk=chunk)
    arranged_data = dask.delayed(arrange_data)(point_data=point_data, bounds=chunk.parent_bounds)
    return dask.delayed(write_tdb)(arranged_data)

def shatter(filename: str, group_size: int, res: float, local: bool, client,
             polygon=None, p_srs=None, watch=False):
    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

    # set up tiledb
    create_tiledb(bounds)

    start = time.perf_counter_ns()

    l = []
    for ch in bounds.chunk():
        l.append(run_one(reader, ch))
    if local:
        with ProgressBar(), Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
            prof.register()
            rprof.register()
            cprof.register()
            dask.compute(l, traverse=True, optimize_graph=True)[0]

    else:
        futures = []
        if watch:
            dask.compute(l, optimize_graph=True)
            client.gather(futures)
            input("Press 'enter' to finish watching.")
        else:
            with performance_report():
                dask.compute(l, optimize_graph=True)
                # for it in bounds.chunk():
                #     futures.append(client.submit(run_one, reader=reader, chunk=it, local=False))
                # progress(futures)
        # gd = [client.submit(get_data, reader=reader, chunk=ch) for ch in bounds.chunk()]
        # ad = [client.submit(arrange_data, point_data=d, bounds=bounds) for d in gd]
        # pc = [client.submit(write_tdb, res=d) for d in ad]
        # futures = client.compute(l)

    end = time.perf_counter_ns()
    print("Time", (end-start)/(pow(10,9)))