import time
from os import path

import tiledb
import pdal
import numpy as np

import dask
import dask.array as da
from dask.distributed import performance_report, progress, Client, as_completed

from .bounds import Bounds, Chunk, create_bounds

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

def floor_x(points: da.Array, bounds: Bounds):
    return da.array(da.floor((points - bounds.minx) / bounds.cell_size), np.int32)

def floor_y(points: da.Array, bounds: Bounds):
    return da.array(da.floor((points - bounds.miny) / bounds.cell_size), np.int32)

def get_zs(points: da.Array, chunk: Chunk, bounds: Bounds):
    xypoints = points[['X','Y']].view()
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

    write_tdb(tdb, [ dx, dy, dd ])

def shatter(filename: str, tdb_dir: str, group_size: int, res: float,
            debug: bool, client=None, polygon=None, watch=False):

    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon)

    # set up tiledb
    config = create_tiledb(bounds, tdb_dir)

    # Begin main operations
    start = time.perf_counter_ns()
    with tiledb.open(tdb_dir, "w", config=config) as tdb:
        t = tdb if debug else client.scatter(tdb)

        # Create method collection for dask to compute
        l = []
        chunks = bounds.chunk(filename)

        for ch in chunks:
            l.append(arrange_data(reader, ch, t))

        if debug:
            data_futures = dask.compute(l, optimize_graph=True)[0]
        else:
            with performance_report(f'{tdb_dir}-dask-report.html'):
                data_futures = client.compute(l, optimize_graph=True)
                progress(data_futures)


        end = time.perf_counter_ns()
        print("Done in", (end-start)/10**9, "seconds")
        if watch:
            input("Press 'enter' to finish watching.")

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