import numpy as np
import datetime

from silvimetric.resources import Extents
from silvimetric.commands.shatter import run

def check_for_holes(leaves: list[Extents], chunk: Extents):
    ind = np.array([], dtype=chunk.get_indices().dtype)
    for leaf in leaves:
        a = leaf.get_indices()
        if ind.size == 0:
            ind = a
        else:
            ind = np.concatenate([ind, a])
    dx = ind['x']
    dy = ind['y']

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    #if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux)
    for idx, minmax in enumerate(xrange):
        if idx + 1 < len(xrange):
            assert minmax + 1 == xrange[idx + 1], f"Hole in derived extents between {minmax} {xrange[idx + 1]}"

    yrange = np.sort(uy)
    for idx, minmax in enumerate(yrange):
        if idx + 1 < len(yrange):
            assert minmax + 1 == yrange[idx + 1], f"Hole in derived extents between {minmax} {yrange[idx + 1]}"

def check_indexing(extents: Extents, leaf_list):
    # gather indices from the chunks to match with extents
    b_indices = extents.get_indices()
    indices = np.array([], dtype=b_indices.dtype)
    count = 0

    #check that mins and maxes are correct first

    for ch in leaf_list:
        ch: Extents
        if not np.any(indices['x']):
            indices = ch.get_indices()
        else:
            indices = np.append(indices, ch.get_indices())
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


class TestExtents(object):
    def test_indexing(self, extents, filtered, unfiltered):
        check_indexing(extents, filtered)
        check_for_holes(filtered, extents)
        check_indexing(extents, unfiltered)
        check_for_holes(unfiltered, extents)

    # def test_cells(self, copc_filepath, unfiltered, resolution):
    #     flag = False
    #     bad_chunks = []
    #     for leaf in unfiltered:
    #         reader = pdal.Reader(copc_filepath)
    #         crop = pdal.Filter.crop(bounds=str(leaf))
    #         p = reader | crop
    #         count = p.execute()
    #         # idx = leaf.get_indices()
    #         # xs = np.unique(idx['x'])
    #         # ys = np.unique(idx['y'])
    #         # chunk_pc = resolution**2 * xs.size * ys.size
    #         if count == 0:
    #             continue
    #         if count == resolution**2:
    #             continue
    #         else:
    #             flag = True
    #             bad_chunks.append(leaf)
    #     assert flag == False, f"{[str(leaf) for leaf in bad_chunks]} are bad chunks"

    def test_pointcount(self, filtered, unfiltered, test_point_count, shatter_config, storage):

        with storage.open('w') as tdb:
            shatter_config.start_time = datetime.datetime.now().timestamp() * 1000
            fc = run(filtered, shatter_config, storage)
            ufc = run(unfiltered, shatter_config, storage)

            assert fc == ufc, f"""
                Filtered and unfiltered point counts don't match.
                Filtered: {fc}, Unfiltered: {ufc}"""
            assert test_point_count == fc, f"""
                Filtered point counts don't match.
                Expected {test_point_count}, got {fc}"""
            assert test_point_count == ufc, f"""
                Unfiltered point counts don't match.
                Expected {test_point_count}, got {ufc}"""