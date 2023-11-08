import numpy as np
import dask

from treetally.bounds import Bounds
from treetally.shatter import arrange_data, shatter
from itertools import chain

def check_for_holes(leaves, chunk):
    c = np.copy(leaves)
    dx = c[:, 0:2]
    dy = c[:, 2:4]

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    #check that edges are the same
    assert ux.min() == chunk.minx, f"Derived minx ({ux.min()}) doesn't match bounds minx ({chunk.minx})."
    assert ux.max() >= chunk.maxx, f"Derived maxx ({ux.max()}) doesn't match bounds maxx ({chunk.maxx})."

    assert uy.min() <= chunk.miny, f"Derived miny ({uy.min()}) doesn't match bounds miny ({chunk.miny})."
    assert uy.max() == chunk.maxy, f"Derived maxy ({uy.max()}) doesn't match bounds maxy ({chunk.maxy})."

    #if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux, axis=0)
    for idx, minmax in enumerate(xrange):
        if idx + 1 < len(xrange):
            assert minmax[1] == xrange[idx + 1][0], f"Hole in derived bounds between {minmax[1]} {xrange[idx][0]}"

    yrange = np.sort(uy, axis=0)
    for idx, minmax in enumerate(yrange):
        if idx + 1 < len(yrange):
            assert minmax[1] == yrange[idx + 1][0], f"Hole in derived bounds between {minmax[1]} {yrange[idx][0]}"

def check_indexing(bounds, leaf_list):
    # gather indices from the chunks to match with bounds
    indices = np.array([], dtype=bounds.indices.dtype)
    b_indices = bounds.indices
    count = 0

    #check that mins and maxes are correct first

    for ch in leaf_list:
        if not np.any(indices['x']):
            indices = ch.indices
        else:
            indices = np.append(indices, ch.indices)
        count += 1

    assert b_indices['x'].min() == indices['x'].min(), f"""X Minimum indices do not match. \
    Min derived: {indices['x'].min()}
    Min base: {b_indices['x'].min()}
    """

    assert b_indices['x'].max() == indices['x'].max(), f"""X Maximum indices do not match. \
    Max derived: {indices['x'].max()}
    Max base: {b_indices['x'].max()}
    """

    assert b_indices['y'].min() == indices['y'].min(), f"""Y Minimum indices do not match. \
    Min derived: {indices['y'].min()}
    Min base: {b_indices['y'].min()}
    """

    assert b_indices['y'].max() == indices['y'].max(), f"""Y Maximum indices do not match. \
    Max derived: {indices['y'].max()}
    Max base: {b_indices['y'].max()}
    """

    # check that all original indices are in derived indices
    for xy in b_indices:
        assert xy in indices, f"Derived indices missing index: {xy}"

    for xy in indices:
        assert xy in b_indices, f"Derived indices created index outside of bounds: {xy}"

    u, c = np.unique(np.array(indices), return_counts=True)
    dup = u[c > 1]
    b = np.any([dup['x'], dup['y']])

    assert b == False, f"Indices duplicated: {dup}"


class TestChunk(object):

    # def compare_bounds(self, b0, b1):
    #     assert b0.minx == b1.minx
    #     assert b0.maxx == b1.maxx
    #     assert b0.miny == b1.miny
    #     assert b0.maxy == b1.maxy

    # def test_split(self, chunk, resolution, group_size, srs):
    #     c0b = Bounds(chunk.minx, chunk.miny, chunk.midx, chunk.midy, resolution, group_size, srs)
    #     c1b = Bounds(chunk.midx, chunk.miny, chunk.maxx, chunk.midy, resolution, group_size, srs)
    #     c2b = Bounds(chunk.minx, chunk.midy, chunk.midx, chunk.maxy, resolution, group_size, srs)
    #     c3b = Bounds(chunk.midx, chunk.midy, chunk.maxx, chunk.maxy, resolution, group_size, srs)
    #     chunks = [ch for ch in chunk.split()]

    #     self.compare_bounds(chunks[0], c0b)
    #     self.compare_bounds(chunks[1], c1b)
    #     self.compare_bounds(chunks[2], c2b)
    #     self.compare_bounds(chunks[3], c3b)
    #     # assert chunks[0].bounds == c0b
    #     # assert chunks[1].bounds == c1b
    #     # assert chunks[2].bounds == c2b
    #     # assert chunks[3].bounds == c3b

    def test_indexing(self, filepath, bounds):
        leaf_list = list(bounds.chunk(filepath, 100))
        unfiltered = list(bounds.root_chunk.get_leaf_children())
        check_indexing(bounds, leaf_list)
        check_indexing(bounds, unfiltered)


    # sub chunks should all add up to exactly what their parent is
    # original chunk will be expanded to fit the cell size
    # def test_chunking(self, bounds):
    #     leaves = chunk.get_leaf_children()

    #     check_for_holes(leaves, chunk)

    # def test_filtering(self, filepath, chunk):

    #     f = chunk.filter(filepath, 3000)

    #     leaf_list = get_leaves(f)

    #     leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in leaf_list])[0]
    #     leaves = np.array([ch for leaf in leaf_procs for ch in leaf], dtype=np.float64)
    #     check_for_holes(leaves, chunk)

    # def test_bounds_to_chunk(self, chunk, filepath):
    #     import pdal

    #     bounds_reader = pdal.Reader(filepath)
    #     bounds_reader._options['bounds'] = str(chunk.root_bounds)
    #     bpc = bounds_reader.pipeline().execute()

    #     chunk_reader = pdal.Reader(filepath)
    #     chunk_reader._options['bounds'] = str(chunk.bounds)
    #     cpc = chunk_reader.pipeline().execute()

    #     assert cpc == bpc, f"Points found with original bounds({chunk.root_bounds}): {bpc} does not match points found with adjusted chunk bounds({chunk.bounds}): {cpc}"

    def test_pointcount(self, pipeline, bounds, filepath, test_point_count):

        filtered = list(bounds.chunk(filepath, 100))

        l1 = [arrange_data(pipeline, leaf, ['Z']) for leaf in filtered]
        filtered_counts = dask.compute(*l1, optimize_graph=True)

        unfiltered = list(bounds.root_chunk.get_leaf_children())
        l2 = [arrange_data(pipeline, leaf, ['Z']) for leaf in unfiltered]
        unfiltered_counts = dask.compute(*l2, optimize_graph=True)

        fc = sum(filtered_counts)
        ufc = sum(unfiltered_counts)

        assert fc == ufc, f"""
            Filtered and unfiltered point counts don't match.
            Filtered: {fc}, Unfiltered: {ufc}"""
        assert test_point_count == fc, f"""
            Filtered point counts don't match.
            Expected {test_point_count}, got {fc}"""
        assert test_point_count == ufc, f"""
            Unfiltered point counts don't match.
            Expected {test_point_count}, got {ufc}"""