import dask
import pdal
import numpy as np

from treetally.chunk import get_leaves
from treetally.shatter import arrange_data

def check_for_holes(leaves, chunk):
    c = np.copy(leaves)
    dx = c[:, 0:2]
    dy = c[:, 2:4]

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    assert(ux.min() == chunk.minx,
        f"Derived minx ({ux.min()}) doesn't match bounds minx ({chunk.minx}).")
    assert(ux.max() == chunk.maxx,
        f"Derived minx ({ux.max()}) doesn't match bounds minx ({chunk.maxx}).")

    assert(uy.min() == chunk.miny,
        f"Derived miny ({uy.min()}) doesn't match bounds minx ({chunk.miny}).")
    assert(uy.max() == chunk.maxy,
        f"Derived minx ({uy.max()}) doesn't match bounds minx ({chunk.maxy}).")

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
def test_chunking(chunk):
    leaves = chunk.get_leaf_children()

    check_for_holes(leaves, chunk)

def test_filtering(test_pointcloud, chunk):

    f = chunk.filter(test_pointcloud, 3000)

    leaf_list = get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaf_list])[0]
    leaves = np.array([ch for leaf in leaf_procs for ch in leaf], dtype=np.float64)
    check_for_holes(leaves, chunk)

def test_pointcount(pipeline, chunk, test_pointcloud, test_point_count):
    f = chunk.filter(test_pointcloud)

    leaf_list = get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaf_list])[0]
    l = [arrange_data(pipeline, ch, chunk.root_bounds, ['Z']) for leaf in leaf_procs for ch in leaf]
    counts = dask.compute(*l, optimize_graph=True)
    count = 0
    for a in counts:
        count += a.sum()
    assert(count == test_point_count, f"Point counts don't match. Expected {test_point_count}, got {count}")