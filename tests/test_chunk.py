import numpy as np
import dask

from treetally.chunk import get_leaves
from treetally.bounds import create_bounds
from treetally.shatter import arrange_data, shatter

def check_for_holes(leaves, chunk):
    c = np.copy(leaves)
    dx = c[:, 0:2]
    dy = c[:, 2:4]

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    assert ux.min() == chunk.minx, f"Derived minx ({ux.min()}) doesn't match bounds minx ({chunk.minx})."
    assert ux.max() == chunk.maxx, f"Derived minx ({ux.max()}) doesn't match bounds minx ({chunk.maxx})."

    assert uy.min() == chunk.miny, f"Derived miny ({uy.min()}) doesn't match bounds minx ({chunk.miny})."
    assert uy.max() == chunk.maxy, f"Derived minx ({uy.max()}) doesn't match bounds minx ({chunk.maxy})."

    #if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux, axis=0)
    for idx, minmax in enumerate(xrange):
        if idx + 1 < len(xrange):
            assert minmax[1] == xrange[idx + 1][0], f"Hole in derived bounds between {minmax[1]} {xrange[idx][0]}"

    yrange = np.sort(uy, axis=0)
    for idx, minmax in enumerate(yrange):
        if idx + 1 < len(yrange):
            assert minmax[1] == yrange[idx + 1][0], f"Hole in derived bounds between {minmax[1]} {yrange[idx][0]}"

class TestChunk(object):

    def test_indexing(self, chunk, filepath, bounds):
        f = chunk.filter(filepath)

        leaf_list = get_leaves(f)
        bxin = range(bounds.xi)
        byin = range(bounds.yi)

        indices = np.array([], dtype=chunk.indices.dtype)
        for ch in leaf_list:
            assert all(x in bxin for x in ch.indices['x'])
            assert all(y in byin for y in ch.indices['y'])
            if not np.any(indices['x']):
                indices = ch.indices
            else:
                indices = np.append(indices, ch.indices)


        u, c = np.unique(np.array(indices), return_counts=True)
        dup = u[c > 1]
        b = np.any([dup['x'], dup['y']])

        assert b == False, f"Indices duplicated: {dup}"

        # xu, cxu = np.unique(np.array(cxin), return_counts=True)
        # xdup = xu[cxu > 1]
        # xbool = np.any(xdup)

        # assert xbool == False, f"X indices duplicated. Duplicate indices: {xdup}"

        # yu, cyu = np.unique(np.array(cyin), return_counts=True)
        # ydup = yu[cyu > 1]
        # ybool = np.any(ydup)
        # assert ybool == False, f"Y indices duplicated. Duplicate indices: {ydup}"

    # sub chunks should all add up to exactly what their parent is
    # original chunk will be expanded to fit the cell size
    def test_chunking(self, chunk):
        leaves = chunk.get_leaf_children()

        check_for_holes(leaves, chunk)

    def test_filtering(self, filepath, chunk):

        f = chunk.filter(filepath, 3000)

        leaf_list = get_leaves(f)

        leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaf_list])[0]
        leaves = np.array([ch for leaf in leaf_procs for ch in leaf], dtype=np.float64)
        check_for_holes(leaves, chunk)

    def test_pointcount(self, pipeline, chunk, filepath, test_point_count):
        f = chunk.filter(filepath)

        leaf_list = get_leaves(f)

        leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaf_list])[0]
        l = [arrange_data(pipeline, ch, chunk.root_bounds, ['Z']) for leaf in leaf_procs for ch in leaf]
        counts = dask.compute(*l, optimize_graph=True)
        count = 0
        for a in counts:
            count += a
        assert count == test_point_count, f"Point counts don't match. Expected {test_point_count}, got {count}"