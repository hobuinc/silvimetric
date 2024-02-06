import math
import numpy as np

import logging
import dask
import dask.bag as db

from typing import Self
from math import ceil

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
        self.cell_count = int((self.rangex * self.rangey) / self.resolution ** 2)

        self.x1 = math.floor((minx - self.root.minx) / resolution)
        self.y1 = math.floor((self.root.maxy - maxy) / resolution)
        self.x2 = math.ceil((maxx - self.root.minx) / resolution)
        self.y2 = math.ceil((self.root.maxy - miny) / resolution)

    def get_indices(self):
        return np.array(
            [(i,j) for i in range(self.x1, self.x2)
            for j in range(self.y1, self.y2)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )

    def chunk(self, data: Data, res_threshold=100, pc_threshold=600000,
            depth_threshold=6, scan=False):
        if self.root is not None:
            bminx, bminy, bmaxx, bmaxy = self.root.get()
            r = self.root
        else:
            bminx, bminy, bmaxx, bmaxy = self.bounds.get()
            r = self.bounds

        # make bounds in scale with the desired resolution
        minx = bminx + (self.x1 * self.resolution)
        maxx = bminx + (self.x2 * self.resolution)
        miny = bmaxy - (self.y2 * self.resolution)
        maxy = bmaxy - (self.y1 * self.resolution)

        chunk = Extents(Bounds(minx, miny, maxx, maxy), self.resolution, r)

        if self.bounds == self.root:
            self.root = chunk.bounds

        filtered = []
        curr = db.from_delayed(chunk.filter(data, res_threshold, pc_threshold, depth_threshold))
        curr_depth = 0

        logger = logging.getLogger('silvimetric')
        while curr.npartitions > 0:

            logger.info(f'Filtering {curr.npartitions} tiles at depth {curr_depth}')
            n = curr.compute()
            to_add = [ne for ne in n if isinstance(ne, Extents)]
            if to_add:
                filtered = filtered + to_add

            curr = db.from_delayed([ne for ne in n if not isinstance(ne, Extents)])
            curr_depth += 1

        return filtered


    def split(self):
        minx, miny, maxx, maxy = self.bounds.get()

        x_adjusted = math.floor((maxx - minx) / 2 / self.resolution)
        y_adjusted = math.floor((maxy - miny) / 2 / self.resolution)

        midx = minx + (x_adjusted * self.resolution)
        midy = maxy - (y_adjusted * self.resolution)

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
    def filter(self, data: Data, res_threshold=100, pc_threshold=600000, depth_threshold=6, depth=0) -> Self:


        pc = data.estimate_count(self.bounds)
        target_pc = pc_threshold
        minx, miny, maxx, maxy = self.bounds.get()

        # is it empty?
        if not pc:
            return [ ]
        else:
            # has it hit the threshold yet?
            area = (maxx - minx) * (maxy - miny)
            next_split_x = (maxx-minx) / 2
            next_split_y = (maxy-miny) / 2

            # if the next split would put our area below the resolution, or if
            # the point count is less than the threshold (600k) then use this
            # tile as the work unit.
            if next_split_x < self.resolution or next_split_y < self.resolution:
                return [ self ]
            elif pc < target_pc:
                return [ self ]
            elif area < res_threshold**2 or depth >= depth_threshold:
                pc_per_cell = pc / (area / self.resolution**2)
                cell_estimate = ceil(target_pc / pc_per_cell)

                return self.get_leaf_children(cell_estimate)
            else:
                return [ ch.filter(data, res_threshold, pc_threshold, depth_threshold, depth=depth+1) for ch in self.split() ]


    def _find_dims(self, tile_size):
        s = math.sqrt(tile_size)
        if int(s) == s:
            return [s, s]
        rng = np.arange(1, tile_size+1, dtype=np.int32)
        factors = rng[np.where(tile_size % rng == 0)]
        idx = int((factors.size/2)-1)
        x = factors[idx]
        y = int(tile_size/ x)
        return [x, y]

    def get_leaf_children(self, tile_size):
        res = self.resolution
        xnum, ynum = self._find_dims(tile_size)

        local_xs = np.array([
                [x, min(x+xnum, self.x2)]
                for x in range(self.x1, self.x2, int(xnum))
            ], dtype=np.float64)
        dx = (res * local_xs) + self.root.minx

        local_ys = np.array([
                [min(y+ynum, self.y2), y]
                for y in range(self.y1, self.y2, int(ynum))
            ], dtype=np.float64)
        dy = self.root.maxy - (res * local_ys)

        coords_list = np.array([[*x,*y] for x in dx for y in dy],dtype=np.float64)
        yield from [
            Extents(Bounds(minx, miny, maxx, maxy), self.resolution, self.root)
            for minx,maxx,miny,maxy in coords_list
        ]

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

        return Extents(sub, res, base)

    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds.get()
        return f"([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}])"
