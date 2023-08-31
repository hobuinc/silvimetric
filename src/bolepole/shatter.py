import time
import math
import argparse
import random

import tiledb
import pdal
import numpy as np
from shapely import from_wkt
from pyproj import CRS

import dask
import dask.array as da
from dask.distributed import Client, performance_report, progress
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, ProgressBar, visualize

from .bounds import Bounds, Chunk

def cell_indices(xpoints, ypoints, x, y):
    r =  np.logical_and(xpoints.astype(np.int32) == x, ypoints.astype(np.int32) == y)
    return r

def assign_points(zpoints, boolcells):
    pass

def arrange_data(point_data, bounds: Bounds):
    points: da.Array
    chunk: Chunk

    points, chunk = point_data

    points = da.from_array(points)
    src_crs = bounds.src_srs
    if src_crs != chunk.srs:
        x_points, y_points = bounds.transform(points['X'], points['Y'])
        xis = da.floor((x_points - bounds.minx) / bounds.cell_size)
        yis = da.floor((y_points - bounds.miny) / bounds.cell_size)
    else:
        xis = dask.delayed(da.floor)((points['X'] - bounds.minx) / bounds.cell_size)
        yis = dask.delayed(da.floor)((points['Y'] - bounds.miny) / bounds.cell_size)


    # Set up data object
    dx = np.array([], dtype=np.int32)
    dy = np.array([], dtype=np.int32)
    zs = []
    counts = []
    for x, y in chunk.indices:
        dx = np.append(dx, x)
        dy = np.append(dy, y)
        idx = dask.delayed(cell_indices)(xis, yis, x, y)
        where = dask.delayed(da.where)(idx==True)
        z = dask.delayed(lambda slc, pts: np.array(pts['Z'][slc], object))(slc=where, pts=points)
        c = dask.delayed(lambda z: z.size)(z=z)
        zs.append(z)
        counts.append(c)
    dac = dask.delayed(np.array)(counts, np.int32)
    daz = dask.delayed(lambda zs: np.array([*zs, None],dtype=object)[:-1])(zs=zs)

    # dd = dask.delayed(lambda count, Z: {'count': count, 'Z': Z})(count=dac, Z=daz)
    dd = dask.delayed(lambda x=0: { 'count': np.array([1,2,3]), 'Z':np.array([
        np.zeros((random.randint(500,100000),), object),
        np.zeros((random.randint(500,100000),), object),
        np.zeros((random.randint(500,100000),), object),
        None], object)[:-1]})()
    return dask.delayed([dx, dy, dd])

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)

    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = np.array(pipeline.arrays[0], copy=False)
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
            capacity=10000000000, attrs=[count_att, z_att], allows_duplicates=True)
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

def write_tdb(tdb, res):
    dx, dy, dd = res.compute()
    tdb[dx, dy] = dd
    return dd['count'].sum()

def get_time_string(start, end):
    final_time_s = (end-start)/pow(10,9)

    hrs = final_time_s / 60 / 60
    hrs_flr = math.floor(hrs)

    mins = (hrs - hrs_flr) * 60
    mins_flr = math.floor(mins)

    secs = (mins - mins_flr) * 60
    return f"{hrs_flr}:{mins_flr}:{secs}"

def main_function(filename: str, threads: int, workers: int, group_size: int, res: float,
         local: bool, stats_bool: bool, polygon=None, p_srs=None):


    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

    # set up tiledb
    create_tiledb(bounds)

    # write to tiledb
    with tiledb.SparseArray("stats", "w") as tdb:
        # write_tdb(tdb, [1,2,3])
        # apply metadata
        tdb.meta["LAYER_EXTENT_MINX"] = bounds.minx
        tdb.meta["LAYER_EXTENT_MINY"] = bounds.miny
        tdb.meta["LAYER_EXTENT_MAXX"] = bounds.maxx
        tdb.meta["LAYER_EXTENT_MAXY"] = bounds.maxy
        if (bounds.srs):
            tdb.meta["CRS"] = bounds.srs.to_wkt()

        if stats_bool:
            pc = 0
            cell_count = bounds.xi * bounds.yi
            chunk_count = 0
            start = time.perf_counter_ns()

        it = bounds.chunk()
        l = []
        for ch in it:
            point_data = dask.delayed(get_data)(reader=reader, chunk=ch)
            arranged_data = dask.delayed(arrange_data)(point_data=point_data, bounds=bounds)
            point_count = dask.delayed(write_tdb)(tdb, arranged_data)
            l.append(point_count)
            chunk_count+=1

        if local:
            with  Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
                prof.register()
                rprof.register()
                cprof.register()
                pc = dask.compute(l, traverse=True, optimize_graph=True)
                # prof.visualize(save=True, show=False)
                # visualize([prof], save=True)

        else:
            with performance_report():
                t = (client.scatter(tdb)).result()
                futures = client.compute(l, optimize_graph=True)
                client.gather(futures)
                # progress(futures)

        if stats_bool:
            end = time.perf_counter_ns()
            t = get_time_string(start,end)

            stats = dict(
                time=t, threads=threads, workers=workers,
                group_size=group_size, resolution=res, point_count=sum(pc[0]),
                cell_count=cell_count, chunk_count=chunk_count,
                filename=filename
            )
            print(stats)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--threads", type=int, default=12)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--group_size", type=int, default=3)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--polygon_srs", type=str, default="EPSG:4326")
    parser.add_argument("--local", type=bool, default=False)
    parser.add_argument("--stats", type=bool, default=False)

    args = parser.parse_args()

    filename = args.filename
    threads = args.threads
    workers = args.workers
    group_size = args.group_size
    res = args.resolution
    poly = args.polygon
    p_srs = args.polygon_srs

    local = args.local
    stats_bool = args.stats

    global client
    client = None
    if local:
        dask.config.set(scheduler='processes')
    else:
        client = Client()


    main_function(filename, threads, workers, group_size, res, local, stats_bool,
          poly, p_srs)

if __name__ == "__main__":
    main()