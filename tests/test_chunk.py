import numpy as np
import dask
import pdal

from treetally.bounds import Bounds
from treetally.shatter import arrange_data, shatter

def check_for_holes(leaves: list[Bounds], chunk: Bounds):
    ind = np.array([], dtype=chunk.indices.dtype)
    for leaf in leaves:
        if ind.size == 0:
            ind = leaf.indices
        else:
            ind = np.concatenate([ind, leaf.indices])
    dx = ind['x']
    dy = ind['y']

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    #if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux)
    for idx, minmax in enumerate(xrange):
        if idx + 1 < len(xrange):
            assert minmax + 1 == xrange[idx + 1], f"Hole in derived bounds between {minmax} {xrange[idx + 1]}"

    yrange = np.sort(uy)
    for idx, minmax in enumerate(yrange):
        if idx + 1 < len(yrange):
            assert minmax + 1 == yrange[idx + 1], f"Hole in derived bounds between {minmax} {yrange[idx + 1]}"

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


class TestBounds(object):

    def test_indexing(self, filepath, bounds):
        leaf_list = list(bounds.chunk(filepath, 100))
        unfiltered = list(bounds.root_chunk.get_leaf_children())
        check_indexing(bounds, leaf_list)
        check_for_holes(leaf_list, bounds.root_chunk)
        check_indexing(bounds, unfiltered)

    def test_cells(self, filepath, bounds, group_size, resolution):
        leaf_list = list(bounds.chunk(filepath, 100))
        flag = False
        bad_chunks = []
        for leaf in leaf_list:
            reader = pdal.Reader(filepath)
            reader._options['bounds'] = str(leaf)
            count = reader.pipeline().execute()
            xs = np.unique(leaf.indices['x'])
            ys = np.unique(leaf.indices['y'])
            chunk_pc = (resolution - 1)**2 * xs.size * ys.size
            if count == 0:
                continue
            if count == chunk_pc:
                continue
            else:
                flag = True
                bad_chunks.append(leaf)
        assert flag == False, f"{[str(leaf) for leaf in bad_chunks]}"

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