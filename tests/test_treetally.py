import dask
import pdal
import numpy as np
import itertools
import types

from treetally import Chunk
from treetally import Bounds
from treetally.shatter import arrange_data, get_leaves

global chunklist
res = 100
gs = 16
srs = 2992
minx = 635579.19
maxx = 639003.73
miny = 848887.49
maxy = 853534.37

point_count = 61201

# def get_leaves(c):
#     while True:
#         try:
#             n = next(c)
#             if isinstance(n, types.GeneratorType):
#                 get_leaves(n)
#             elif isinstance(n, Chunk):
#                 chunklist.append(n)
#         except StopIteration:
#             break

def check_for_holes(leaves, chunk):
    c = np.copy(leaves)
    dx = c[:, 0:2]
    dy = c[:, 2:4]

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    assert(ux.min() == minx,
        f"Derived minx ({ux.min()}) doesn't match bounds minx ({minx}).")
    assert(ux.max() == maxx,
        f"Derived minx ({ux.max()}) doesn't match bounds minx ({maxx}).")

    assert(uy.min() == miny,
        f"Derived miny ({uy.min()}) doesn't match bounds minx ({miny}).")
    assert(uy.max() == maxy,
        f"Derived minx ({uy.max()}) doesn't match bounds minx ({maxy}).")

    #if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux, axis=0)
    for idx, minmax in enumerate(xrange):
        if idx < len(xrange):
            assert(minmax[1] == xrange[idx][0],
                f"Hole in derived bounds between {minmax[1]} {xrange[idx][0]}")

    yrange = np.sort(uy, axis=0)
    for idx, minmax in enumerate(yrange):
        if idx < len(yrange):
            assert(minmax[1] == yrange[idx][0],
                f"Hole in derived bounds between {minmax[1]} {yrange[idx][0]}")

# sub chunks should all add up to exactly what their parent is
# original chunk will be expanded to fit the cell size
def test_chunking():
    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    chunk = Chunk(minx, maxx, miny, maxy, root)
    leaves = chunk.get_leaf_children()

    check_for_holes(leaves, chunk)

def test_pointcount(autzen_classified):
    reader = pdal.Reader(autzen_classified)
    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    c = Chunk(minx, maxx, miny, maxy, root)
    f = c.filter(autzen_classified)

    global chunklist
    chunklist = []
    get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in chunklist])[0]
    l = [arrange_data(reader, ch, root) for leaf in leaf_procs for ch in leaf]
    counts = dask.compute(*l, optimize_graph=True)
    count = 0
    for a in counts:
        count += a.sum()
    assert(count == point_count, f"Point counts don't match. Expected {point_count}, got {count}")

def test_filtering(autzen_classified):

    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    chunk = Chunk(minx, maxx, miny, maxy, root)
    f = chunk.filter(autzen_classified, 3000)

    global chunklist
    chunklist = []
    get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in chunklist])[0]
    leaves = np.array([ch for leaf in leaf_procs for ch in leaf], dtype=np.float64)
    check_for_holes(leaves, chunk)
