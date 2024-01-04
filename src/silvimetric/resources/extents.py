import math
import numpy as np

import dask
import dask.array as da
import dask.bag as db

from typing import Self

from .bounds import Bounds
from .storage import Storage
from .data import Data

class Extents(object):

    def __init__(self,
                 bounds: Bounds,
                 resolution: float,
                 root: Bounds):

        self.bounds = bounds
        self.root = root

        minx, miny, maxx, maxy = self.bounds.get()

        self.rangex = maxx - minx
        self.rangey = maxy - miny
        self.resolution = resolution

        self.x1 = math.floor((minx - self.root.minx) / resolution)
        self.y1 = math.floor((self.root.maxy - maxy) / resolution)
        self.x2 = math.floor((maxx - self.root.minx) / resolution)
        self.y2 = math.floor((self.root.maxy - miny) / resolution)

    def indices(self):
        return np.array(
            [(i,j) for i in range(self.x1, self.x2)
             for j in range(self.y1, self.y2)],
             dtype=[('x', np.int32), ('y', np.int32)]
        )

    def chunk(self, data: Data, threshold=100) :
        if self.root is not None:
            bminx, bminy, bmaxx, bmaxy = self.root.get()
            r = self.root
        else:
            bminx, bminy, bmaxx, bmaxy = self.bounds.get()
            r = self.bounds

        # buffers are only applied if the bounds do not fit on the cell line
        # in which case we're extending the bounds to the next nearest cell line
        x_buf = 1 if bmaxx % self.resolution != 0 else 0
        y_buf = 1 if bmaxy % self.resolution != 0 else 0
        # make bounds in scale with the desired resolution
        minx = bminx + (self.x1 * self.resolution)
        maxx = bminx + ((self.x2 + x_buf) * self.resolution)
        miny = bmaxy - ((self.y2 + y_buf) * self.resolution)
        maxy = bmaxy - (self.y1 * self.resolution)

        chunk = Extents(Bounds(minx, miny, maxx, maxy), self.resolution, r)
        self.root_chunk: Extents = chunk

        if self.bounds == self.root:
            self.root = self.root_chunk.bounds

        filtered = []
        curr = db.from_delayed(chunk.filter(data, threshold))
        while True:
            # prev = curr.compute()

            # to_add = [one for one in curr if isinstance(one, Extents)]
            to_add = curr.filter(lambda x: isinstance(x, Extents)).compute()
            if to_add:
                filtered.append(db.from_sequence(to_add))
                if len(to_add) == curr.npartitions:
                    break

            # nxt = [one for one in curr if not isinstance(one, Extents)]
            curr = db.from_delayed(curr.filter(lambda x: not isinstance(x, Extents)))


        return db.concat(filtered)

    def split(self):
        minx, miny, maxx, maxy = self.bounds.get()
        midx = minx + ((maxx - minx)/ 2)
        midy = miny + ((maxy - miny)/ 2)
        return [
            Extents(Bounds(minx, miny, midx, midy), self.resolution, self.root), #lower left
            Extents(Bounds(midx, miny, maxx, midy), self.resolution, self.root), #lower right
            Extents(Bounds(minx, midy, midx, maxy), self.resolution, self.root), #top left
            Extents(Bounds(midx, midy, maxx, maxy), self.resolution, self.root)  #top right
        ]

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    @dask.delayed
    def filter(self, data: Data, threshold_resolution=100) -> Self:
        res = max(self.resolution, threshold_resolution)

        pc = data.estimate_count(self.bounds)
        # pc = qi['num_points']
        minx, miny, maxx, maxy = self.bounds.get()

        # is it empty?
        if not pc:
            return [ ]
        else:
            # has it hit the threshold yet?
            xside = maxx - minx
            next_split_x = (maxx-minx) / 2
            next_split_y = (maxy-miny) / 2

            # if the next split would put our area below the resolution, or if
            # the point count is less than the threshold (600k) then use this
            # tile as the work unit.
            if next_split_x < self.resolution or next_split_y < self.resolution:
                return [ self ]
            if pc < 6*10**5:
                return [ self ]
            else:
                return [ ch.filter(data, res) for ch in self.split() ]

    @staticmethod
    def from_storage(tdb_dir: str):
        storage = Storage.from_db(tdb_dir)
        meta = storage.getConfig()
        return Extents(meta.root, meta.resolution, meta.root)

    @staticmethod
    def from_sub(tdb_dir: str, sub: Bounds):

        storage = Storage.from_db(tdb_dir)

        meta = storage.getConfig()
        res = meta.resolution
        base_extents = Extents(meta.root, res, meta.root)
        base = base_extents.bounds

        if sub.minx <= base.minx:
            minx = base.minx
        else:
            minx = base.minx + (math.floor((sub.minx-base.minx)/res) * res)
        if sub.maxx >= base.maxx:
            maxx = base.maxx
        else:
            maxx = base.minx + (math.floor((sub.maxx-base.minx)/res) * res)
        if sub.miny <= base.miny:
            miny = base.miny
        else:
            miny = base.maxy - (math.floor(base.maxy-sub.miny)/res) * res
        if sub.maxy >= base.maxy:
            maxy = base.maxy
        else:
            maxy = base.maxy - math.floor((base.maxy-sub.maxy)/res) * res

        new_b = Bounds(minx, miny, maxx, maxy)
        return Extents(new_b, res, base)

    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds.get()
        return f"([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}])"
