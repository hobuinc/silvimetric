from typing import List
import numpy as np
import datetime

from silvimetric import Extents
from silvimetric.commands.shatter import run
from silvimetric.resources.config import ShatterConfig
from silvimetric.resources.storage import Storage



def check_for_overlap(leaves: list[Extents], chunk: Extents):
    for l1 in leaves:
        for l2 in leaves:
            if l2 == l1:
                continue
            assert l1.disjoint(l2)


def check_for_holes(leaves: list[Extents], chunk: Extents):
    ind = np.array([], dtype=chunk.get_indices().dtype)
    for leaf in leaves:
        a = leaf.get_indices()
        if ind.size == 0:
            ind = a
        else:
            ind = np.concatenate([ind, a])
    dx = ind[:,0]
    dy = ind[:,1]

    ux = np.unique(dx, axis=0)
    uy = np.unique(dy, axis=0)

    # if min of this index doesn't fit max of next then there are holes
    xrange = np.sort(ux)
    for idx, minmax in enumerate(xrange):
        if idx + 1 < len(xrange):
            assert minmax + 1 == xrange[idx + 1], (
                f'Hole in derived extents between {minmax} {xrange[idx + 1]}'
            )

    yrange = np.sort(uy)
    for idx, minmax in enumerate(yrange):
        if idx + 1 < len(yrange):
            assert minmax + 1 == yrange[idx + 1], (
                f'Hole in derived extents between {minmax} {yrange[idx + 1]}'
            )


def check_indexing(extents: Extents, leaf_list):
    # gather indices from the chunks to match with extents
    b_indices = extents.get_indices()
    indices = np.array([], dtype=b_indices.dtype)
    count = 0

    # check that mins and maxes are correct first

    for ch in leaf_list:
        ch: Extents
        if not np.any(indices):
            indices = ch.get_indices()
        else:
            indices = np.vstack((indices, ch.get_indices()))
        count += 1

    assert (
        b_indices[:,0].min() == indices[:,0].min()
    ), f"""X Minimum indices do not match. \
    Min derived: {indices[:,0].min()}
    Min base: {b_indices[:,0].min()}
    """

    assert (
        b_indices[:,0].max() == indices[:,0].max()
    ), f"""X Maximum indices do not match. \
    Max derived: {indices[:,0].max()}
    Max base: {b_indices[:,0].max()}
    """

    assert (
        b_indices[:,1].min() == indices[:,1].min()
    ), f"""Y Minimum indices do not match. \
    Min derived: {indices[:,1].min()}
    Min base: {b_indices[:,1].min()}
    """

    assert (
        b_indices[:,1].max() == indices[:,1].max()
    ), f"""Y Maximum indices do not match. \
    Max derived: {indices[:,1].max()}
    Max base: {b_indices[:,1].max()}
    """

    # check that all original indices are in derived indices
    for xy in b_indices:
        assert xy in indices, f'Derived indices missing index: {xy}'

    for xy in indices:
        assert xy in b_indices, (
            f'Derived indices created index outside of bounds: {xy}'
        )


class TestExtents(object):
    def test_indexing(
        self,
        extents: Extents,
        filtered: List[Extents],
        unfiltered: List[Extents],
    ):
        check_indexing(extents, filtered)
        check_for_holes(filtered, extents)
        check_for_overlap(filtered, extents)
        check_indexing(extents, unfiltered)
        check_for_holes(unfiltered, extents)
        check_for_overlap(unfiltered, extents)

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
    #     assert flag == False, f"{[str(leaf) for leaf in bad_chunks]} are bad"

    def test_pointcount(
        self,
        filtered: List[Extents],
        unfiltered: List[Extents],
        test_point_count: int,
        shatter_config: ShatterConfig,
        storage: Storage,
    ):
        with storage.open('w'):
            shatter_config.start_time = (
                datetime.datetime.now().timestamp() * 1000
            )
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
