import time
from os import path

import tiledb
import pdal
import numpy as np

import dask
import dask.array as da
from dask.diagnostics import ProgressBar
from dask.distributed import performance_report, progress, Client

from .bounds import Bounds, create_bounds
from .chunk import Chunk

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
def arrange_data(reader, bounds: list[float], root_bounds: Bounds, tdb=None):
    chunk = Chunk(*bounds, root=root_bounds)
    points = get_data(reader, chunk)
    if not points.size:
        return np.array([0], np.int32)

    zs = get_zs(points, chunk, root_bounds)
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': zs }

    dx = chunk.indices['x']
    dy = chunk.indices['y']
    if tdb:
        write_tdb(tdb, [ dx, dy, dd ])
    return counts

def shatter(filename: str, tdb_dir: str, group_size: int, res: float,
            debug: bool, client=None, polygon=None, watch=False):

    client:Client = client
    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon)

    # set up tiledb
    config = create_tiledb(bounds, tdb_dir)

    # Begin main operations
    with tiledb.open(tdb_dir, "w", config=config) as tdb:
        start = time.perf_counter_ns()

        # Create method collection for dask to compute
        l = []
        if debug:
            # with ProgressBar():
                c = Chunk(bounds.minx, bounds.maxx, bounds.miny, bounds.maxy, bounds)
                f = dask.compute(c.filter(filename))
                leaves = c.get_leaves()
                leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaves])[0]
                l = [arrange_data(reader, ch, bounds, tdb) for leaf in leaf_procs for ch in leaf]
                counts = dask.compute(*l, optimize_graph=True)
        else:
            with performance_report(f'{tdb_dir}-dask-report.html'):
                t = client.scatter(tdb)
                b = client.scatter(bounds)

                # chunks = bounds.chunk(filename=filename)
                c = Chunk(bounds.minx, bounds.maxx, bounds.miny, bounds.maxy, bounds)
                f = client.compute(c.filter(filename), sync=True)
                leaves = c.get_leaves()
                # for leaf in leaves:
                #     print(leaf.get_leaf_children())
                leaf_procs = client.compute([node.get_leaf_children() for node in leaves])[0]

                data_futures = [
                    client.submit(arrange_data, reader=reader, bounds=ch, root_bounds=b, tdb=t)
                    for leaf in leaf_procs for ch in leaf
                ]
                progress(data_futures)

                # l = [arrange_data(reader, ch, bounds, tdb) for leaf in leaf_procs for ch in leaf]
                # for ch in chunks:
                #     l.append(arrange_data(reader, ch, b, t))

                # data_futures = client.compute(l, optimize_graph=True)
                # progress(data_futures)

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