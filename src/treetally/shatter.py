import time
import types
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
    return da.array(da.floor((points - bounds.minx) / bounds.cell_size),
        np.int32)

def floor_y(points: da.Array, bounds: Bounds):
    return da.array(da.floor((bounds.maxy - points) / bounds.cell_size),
        np.int32)

def get_zs(points: da.Array, xis: da.Array, yis: da.Array, chunk: Chunk,
    bounds: Bounds):
    # Set up data object
    zs = dask.compute([da.array(points['Z'][cell_indices(xis, yis, x, y)],
        np.float64) for x,y in chunk.indices], scheduler="threads")[0]
    return np.array([*[z for z in zs], None], object)[:-1]

def get_hag(points: da.Array, xis: da.Array, yis: da.Array, chunk: Chunk,
    bounds: Bounds):
    hag = dask.compute([da.array(points['HeightAboveGround'][cell_indices(xis,
        yis, x, y)], np.float32) for x,y in chunk.indices ],
        scheduler="threads")[0]
    return np.array([*[h for h in hag], None], object)[:-1]


def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)
    # remember that readers.copc is a thread hog
    reader._options['threads'] = 2
    smrf = pdal.Filter.smrf()
    hag = pdal.Filter.hag_nn()
    pipeline = reader | smrf | hag
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

    xypoints = points[['X','Y']].view()
    xis = floor_x(xypoints['X'], root_bounds)
    yis = floor_y(xypoints['Y'], root_bounds)
    # att_data = dask.compute([da.array(points[:][cell_indices(xis,
    #     yis, x, y)], dtype=points.dtype) for x,y in chunk.indices ],
    #     scheduler="threads")[0]

    zs = get_zs(points, xis, yis, chunk, root_bounds)
    hags = get_hag(points, xis, yis, chunk, root_bounds)
    counts = np.array([z.size for z in zs], np.int32)
    dd = {'count': counts, 'Z': zs , 'HeightAboveGround': hags}

    dx = chunk.indices['x']
    dy = chunk.indices['y']
    if tdb:
        write_tdb(tdb, [ dx, dy, dd ])
    return counts

def shatter(filename: str, tdb_dir: str, group_size: int, res: float,
            debug: bool, client=None, polygon=None, watch=False):

    client:Client = client
    # read pointcloud
    reader = pdal.Reader()
    bounds = create_bounds(reader, res, group_size, polygon)

    # set up tiledb
    config = create_tiledb(bounds, tdb_dir)

    global chunklist
    # Begin main operations
    with tiledb.open(tdb_dir, "w", config=config) as tdb:
        start = time.perf_counter_ns()

        # debug uses single threaded dask
        if debug:
            c = Chunk(bounds.minx, bounds.maxx, bounds.miny, bounds.maxy,
                bounds)
            f = c.filter(filename)

            chunklist = []
            get_leaves(f)

            leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in
                chunklist])[0]
            l = [arrange_data(reader, ch, bounds, tdb) for leaf in leaf_procs
                for ch in leaf]
            dask.compute(*l, optimize_graph=True)
        else:
            with performance_report(f'{tdb_dir}-dask-report.html'):
                print('Filtering out empty chunks...')
                t = client.scatter(tdb)
                b = client.scatter(bounds)

                c = Chunk(bounds.minx, bounds.maxx, bounds.miny, bounds.maxy,
                    bounds)
                f = c.filter(filename)

                chunklist = []
                get_leaves(f)
                leaf_procs = client.compute([node.get_leaf_children() for node
                    in chunklist])

                print('Fetching and arranging data...')
                l = [arrange_data(reader, ch, bounds, tdb) for leaf in
                    leaf_procs for ch in leaf]
                data_futures = client.compute(l, optimize_graph=True)

                progress(data_futures)
                client.gather(data_futures)

        end = time.perf_counter_ns()
        print("Done in", (end-start)/10**9, "seconds")
        return


def get_leaves(c):
    while True:
        try:
            n = next(c)
            if isinstance(n, types.GeneratorType):
                get_leaves(n)
            elif isinstance(n, Chunk):
                chunklist.append(n)
        except StopIteration:
            break

def create_tiledb(bounds: Bounds, dirname, atts):
    if tiledb.object_type(dirname) == "array":
        with tiledb.open(dirname, "d") as A:
            A.query(cond="X>=0").submit()
    else:
        dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        # names = atts.names
        # tdb_atts = [tiledb.Attr(name=name, dtype=names[name], var=True, fill=np.dtype()) for name in names]

        z_att = tiledb.Attr(name="Z", dtype=np.float64, var=True,
            fill=float('nan'))
        hag_att = tiledb.Attr(name="HeightAboveGround", dtype=np.float32,
            var=True, fill=float('nan'))

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=10000000000, attrs=[count_att, z_att, hag_att], allows_duplicates=True)
        schema.check()
        tiledb.SparseArray.create(dirname, schema)

    return tiledb.Config({
        "sm.check_coord_oob": False,
        "sm.check_global_order": False,
        "sm.check_coord_dedups": False,
        "sm.dedup_coords": False
    })