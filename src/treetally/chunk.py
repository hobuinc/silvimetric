import math
import types
from itertools import chain

import dask
import pdal
import dask.array as da
import numpy as np

from .bounds import Bounds

class Chunk(object):
    def __init__(self, minx, maxx, miny, maxy, root: Bounds):
        cell_size = root.cell_size
        self.x1 = math.floor((minx - root.minx) / cell_size)
        self.y1 = math.floor((miny - root.miny) / cell_size)

        self.x2 = math.floor(((maxx - root.minx) / cell_size))
        self.y2 = math.floor(((maxy - root.miny) / cell_size))

        # make bounds in scale with the desired resolution
        self.minx = self.x1 * cell_size + root.minx
        self.miny = self.y1 * cell_size + root.miny

        self.maxx = (self.x2 * cell_size) + root.minx
        self.maxy = (self.y2 * cell_size) + root.miny

        self.midx = self.minx + ((self.maxx - self.minx)/ 2)
        self.midy = self.miny + ((self.maxy - self.miny)/ 2)

        self.root_bounds = root
        group_size = root.group_size


        self.bounds = Bounds(self.minx, self.miny, self.maxx, self.maxy,
                             cell_size, group_size, root.srs.to_wkt())

        self.indices = np.array(
            [(i,j) for i in range(self.x1, self.x2)
            for j in range(self.y1, self.y2)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    def filter(self, filename, threshold=1000):
        reader = pdal.Reader(filename)
        reader._options['bounds'] = str(self.bounds)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']

        # is it empty?
        if not pc:
            yield None
        else:
            # has it hit the threshold yet?
            area = (self.maxx - self.minx) * (self.maxy - self.miny)
            t = threshold**2
            if area < t:
                yield self
            else:
                children = self.split()
                yield from [c.filter(filename,threshold) for c in children]

    def split(self):
        yield from [
            Chunk(self.minx, self.midx, self.miny, self.midy, self.root_bounds), #lower left
            Chunk(self.midx, self.maxx, self.miny, self.midy, self.root_bounds), #lower right
            Chunk(self.minx, self.midx, self.midy, self.maxy, self.root_bounds), #top left
            Chunk(self.midx, self.maxx, self.midy, self.maxy, self.root_bounds)  #top right
        ]

    def get_leaves(self):
        if self.leaf:
            yield self
        elif not self.empty:
            for child in self.children:
                yield from child.get_leaves()

    def find_dims(self, gs):
        s = math.sqrt(gs)
        if int(s) == s:
            return [s, s]
        rng = np.arange(1, gs+1, dtype=np.int32)
        factors = rng[np.where(gs % rng == 0)]
        idx = int((factors.size/2)-1)
        x = factors[idx]
        y = int(gs / x)
        return [x, y]

    def get_leaf_children(self):
        res = self.root_bounds.cell_size
        gs = self.root_bounds.group_size
        xnum, ynum = self.find_dims(gs)
        # xnum = int(xnum)
        # ynum = int(ynum)

        # xcount = (self.maxx - self.minx) / (xnum * res)
        # ycount = (self.maxy - self.miny) / (ynum * res)
        # print(xcount * ycount)

        # dx = (np.array([[x, min(x+xnum, self.x2)] for x in range(self.x1, self.x2+int(xnum), int(xnum))], dtype=np.float64) * res) + self.root_bounds.minx
        # dy = (np.array([[y, min(y+ynum, self.y2)] for y in range(self.y1, self.y2+int(ynum), int(ynum))], dtype=np.float64) * res) + self.root_bounds.miny
        dx = (np.array([[x, min(x+xnum, self.x2)] for x in range(self.x1, self.x2, int(xnum))], dtype=np.float64) * res) + self.root_bounds.minx
        dy = (np.array([[y, min(y+ynum, self.y2)] for y in range(self.y1, self.y2, int(ynum))], dtype=np.float64) * res) + self.root_bounds.miny
        return np.array([[*x,*y] for x in dx for y in dy],dtype=np.float64)

