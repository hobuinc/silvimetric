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
from dask.distributed import Client, performance_report, progress, LocalCluster, PipInstall, UploadDirectory
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
    where = da.where(idx==True)
    return da.array(points['Z'][where], np.float64)

def get_zs(points, chunk, bounds):
    src_srs = bounds.src_srs
    dst_srs = chunk.srs

    x_points, y_points = xform(src_srs, dst_srs, points, bounds)
    xis = floor_x(x_points, bounds)
    yis = floor_y(y_points, bounds)

    # Set up data object
    zs = []
    for x, y in chunk.indices:
        idx = cell_indices(xis, yis, x, y)
        z = where_true(points, idx)
        # z = da.array(points['Z'][where], object)
        zs.append(z)
        # counts.append(z.size)

        # counts.append(dask.delayed(lambda z: z.size)(z=z))
    return dask.compute(zs)[0]

def arrange_data(point_data: tuple[da.Array, Chunk], bounds: Bounds):
    points, chunk = point_data

    zs = get_zs(points, chunk, bounds)
    npz = np.array([*[np.array(z, np.float64) for z in zs], None], dtype=object)[:-1]
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': npz }
    # dd = dask.delayed(
    #     lambda zs:
    #         {
    #             'count': da.array([z.size for z in zs]),
    #             'Z': np.array([
    #                 *[np.array(z, np.float64) for z in zs],
    #                 None
    #             ], dtype=object)[:-1]
    #         }
    # )(zs=zs)
    # counts = da.from_delayed(value=[z.size for z in zs], shape=(len(zs)))

    # dask.compute(zs)

    dx = da.array([], np.int32)
    dy = da.array([], np.int32)
    for x, y in chunk.indices:
        dx = da.append(dx, x)
        dy = da.append(dy, y)

    return [
        dx,
        dy,
        dd
    ]

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

def run_one(reader, chunk):
    point_data = dask.delayed(get_data)(reader=reader, chunk=chunk)
    arranged_data = dask.delayed(arrange_data)(point_data=point_data, bounds=chunk.parent_bounds)
    point_count = dask.delayed(write_tdb)(arranged_data)
    return point_count.compute()

def main_function(filename: str, group_size: int, res: float,
         local: bool, polygon=None, p_srs=None, watch=False):


    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

    # set up tiledb
    create_tiledb(bounds)

    # write to tiledb

    if local:
        with ProgressBar(), Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
            l = []
            for ch in bounds.chunk():
                point_data = dask.delayed(get_data)(reader=reader, chunk=ch)
                arranged_data = dask.delayed(arrange_data)(point_data=point_data, bounds=bounds)
                point_count = dask.delayed(write_tdb)(arranged_data)
                l.append(point_count)
            prof.register()
            rprof.register()
            cprof.register()
            dask.compute(l, traverse=True, optimize_graph=True)[0]
            # prof.visualize(save=True, show=False)
            # visualize([prof], save=True)

    else:
        futures = []
        for it in bounds.chunk():
            futures.append(client.submit(run_one, reader=reader, chunk=it))
        # gd = [client.submit(get_data, reader=reader, chunk=ch) for ch in bounds.chunk()]
        # ad = [client.submit(arrange_data, point_data=d, bounds=bounds) for d in gd]
        # pc = [client.submit(write_tdb, res=d) for d in ad]
        # futures = client.compute(l)
        client.gather(futures)
        if watch:
            input("Press enter to finish watching.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--group_size", type=int, default=3)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--polygon_srs", type=str, default="EPSG:4326")
    parser.add_argument("--local", type=bool, default=False)
    parser.add_argument("--watch", type=bool, default=False)

    args = parser.parse_args()

    filename = args.filename
    threads = args.threads
    workers = args.workers
    group_size = args.group_size
    res = args.resolution
    poly = args.polygon
    p_srs = args.polygon_srs

    local = args.local
    watch = args.watch

    global client
    client = None
    if local:
        if threads == 1:
            dask.config.set(scheduler="single-threaded")
        else:
            dask.config.set(scheduler='processes')
    else:
        client = Client(n_workers=workers, set_as_default=False)
        dask.config.set(scheduler='threads')
        if watch:
            webbrowser.open(client.cluster.dashboard_link)


    main_function(filename, group_size, res, local, poly, p_srs, watch)

if __name__ == "__main__":
    main()