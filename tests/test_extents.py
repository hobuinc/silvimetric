from typing import List
import numpy as np
import datetime
import itertools
import copy

from silvimetric import Extents, Data
from silvimetric.commands.shatter import run
from silvimetric.resources.config import ShatterConfig
from silvimetric.resources.storage import Storage, StorageConfig, Bounds


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
    dx = ind[:, 0]
    dy = ind[:, 1]

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
        b_indices[:, 0].min() == indices[:, 0].min()
    ), f"""X Minimum indices do not match. \
    Min derived: {indices[:, 0].min()}
    Min base: {b_indices[:, 0].min()}
    """

    assert (
        b_indices[:, 0].max() == indices[:, 0].max()
    ), f"""X Maximum indices do not match. \
    Max derived: {indices[:, 0].max()}
    Max base: {b_indices[:, 0].max()}
    """

    assert (
        b_indices[:, 1].min() == indices[:, 1].min()
    ), f"""Y Minimum indices do not match. \
    Min derived: {indices[:, 1].min()}
    Min base: {b_indices[:, 1].min()}
    """

    assert (
        b_indices[:, 1].max() == indices[:, 1].max()
    ), f"""Y Maximum indices do not match. \
    Max derived: {indices[:, 1].max()}
    Max base: {b_indices[:, 1].max()}
    """

    comps = np.sort(b_indices[:][:, 1]) == np.sort(indices[:][:, 1])
    assert comps.all()

    comps = np.sort(b_indices[:][:, 0]) == np.sort(indices[:][:, 0])
    assert comps.all()


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

    def test_pointcount(
        self,
        filtered: List[Extents],
        unfiltered: List[Extents],
        test_point_count: int,
        shatter_config: ShatterConfig,
        storage: Storage,
    ):
        newsc = copy.deepcopy(shatter_config)
        with storage.open('w'):
            shatter_config.start_time = (
                datetime.datetime.now().timestamp() * 1000
            )
            fc = run(filtered, shatter_config, storage)
            ufc = run(unfiltered, newsc, storage)

            assert fc == ufc, f"""
                Filtered and unfiltered point counts don't match.
                Filtered: {fc}, Unfiltered: {ufc}"""
            assert test_point_count == fc, f"""
                Filtered point counts don't match.
                Expected {test_point_count}, got {fc}"""
            assert test_point_count == ufc, f"""
                Unfiltered point counts don't match.
                Expected {test_point_count}, got {ufc}"""

    def test_chunking(
        self, autzen_storage: StorageConfig, autzen_data, threaded_dask
    ):
        ex = Extents(
            autzen_data.bounds,
            autzen_storage.resolution,
            autzen_storage.alignment,
            autzen_storage.root,
        )
        chs = list(ex.chunk(autzen_data, (5 * 10**6)))
        inner_chunks = []
        for ch in chs:
            inner_chunks = inner_chunks + list(
                itertools.chain(ch.chunk(autzen_data))
            )
        for c1 in inner_chunks:
            c1: Extents
            assert all(c1.disjoint(c2) for c2 in inner_chunks if c2 != c1)
        check_for_holes(inner_chunks, ex)
        check_indexing(ex, inner_chunks)

    def test_big(self):
        from pyproj import CRS

        # file = 'https://s3-us-west-2.amazonaws.com/usgs-lidar-public/USGS_LPC_MI_Charlevoix_TL_2018_LAS_2019/ept.json'

        resolution = 5
        alignment = 'AlignToCenter'
        bounds = Bounds(
            **{
                'maxx': -9475016,
                'maxy': 5681265,
                'minx': -9484547,
                'miny': 5652971,
            }
        )
        storage_config = StorageConfig(
            tdb_dir='asdf.tdb',
            root=bounds,
            crs=CRS.from_epsg('3857'),
            resolution=resolution,
        )
        extents = Extents(
            bounds=bounds,
            root=bounds,
            resolution=resolution,
            alignment=alignment,
        )

        tilesize = storage_config.ysize * storage_config.xsize
        tiled_chunks = extents.get_leaf_children(tilesize)
        check_for_holes(tiled_chunks, extents)
        check_indexing(extents, tiled_chunks)
