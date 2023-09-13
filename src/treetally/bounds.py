import math

import dask
import pdal

import numpy as np
import dask.array as da
from pyproj import CRS
from shapely import from_wkt
from itertools import chain

class Bounds(object):
    def __init__(self, minx, miny, maxx, maxy, cell_size, group_size = 3, srs=None):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)
        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        if self.srs.is_geographic:
            raise Exception(f"Bounds SRS({srs}) is geographic.")
        self.epsg = self.srs.to_epsg()

        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.xi = math.ceil(self.rangex / cell_size)
        self.yi = math.ceil(self.rangey / cell_size)
        self.cell_size = cell_size
        self.group_size = group_size

    def chunk(self, filename:str):
        c = Chunk([self.minx, self.maxx], [self.miny, self.maxy], self)
        c.filter(filename)
        c.set_leaves()
        leaves = c.leaves
        for l in c.leaves:
            yield l.get_read_chunks()
        c.get_read_chunks()
        return c.leaves

        # for i in range(0, self.xi):
        #     for j in range(0, self.yi, self.group_size):
        #         top = min(j+self.group_size-1, self.yi)
        #         yield Chunk([i,i], [j,top], self)

    def split(self, x, y):
        """Yields the geospatial bounding box for a given cell set provided by x, y"""

        minx = self.minx + (x * self.cell_size)
        miny = self.miny + (y * self.cell_size)
        maxx = self.minx + ((x+1) * self.cell_size)
        maxy = self.miny + ((y+1) * self.cell_size)
        return Bounds(minx, miny, maxx, maxy, self.cell_size, self.srs)

    def cell_dim(self, x, y):
        b = self.split(x, y)
        xcenter = (b.maxx - b.minx) / 2
        ycenter = (b.maxy - b.miny) / 2
        return [xcenter, ycenter]

    def __repr__(self):
        if self.srs:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}]) / EPSG:{self.epsg}"
        else:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"

class Chunk(object):
    def __init__(self, xrange: list[int], yrange: list[int], parent: Bounds):
        self.minx, self.maxx = xrange
        self.miny, self.maxy = yrange
        self.midx = self.minx + ((self.maxx - self.minx)/ 2)
        self.midy = self.miny + ((self.maxx - self.minx)/ 2)

        self.parent_bounds = parent
        cell_size = parent.cell_size
        group_size = parent.group_size

        self.x1 = math.floor((self.minx - parent.minx) / cell_size)
        self.x2 = math.ceil(((self.maxx - parent.minx) / cell_size) - 1)
        self.y1 = math.floor((self.miny - parent.miny) / cell_size)
        self.y2 = math.ceil(((self.maxy - parent.miny) / cell_size) - 1)

        self.bounds = Bounds(self.minx, self.miny, self.maxx, self.maxy,
                             cell_size, group_size, parent.srs.to_wkt())

        self.indices = np.array(
            [(i,j) for i in range(self.x1, self.x2+1)
            for j in range(self.y1, self.y2+1)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )
        self.leaf = False

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    def filter(self, filename):
        reader = pdal.Reader(filename)
        reader._options['bounds'] = str(self.bounds)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']

        if not pc:
            self.empty = True
            return

        self.empty = False

        t = 500
        if self.maxx - self.minx < t or self.maxy - self.miny < t :
            self.leaf = True
            return

        self.children = [
            Chunk([self.minx, self.midx], [self.miny, self.midy], self.parent_bounds), #lower left
            Chunk([self.midx, self.maxx], [self.miny, self.midy], self.parent_bounds), #lower right
            Chunk([self.minx, self.midx], [self.midy, self.maxy], self.parent_bounds), #top left
            Chunk([self.midx, self.maxx], [self.midy, self.maxy], self.parent_bounds)  #top right
        ]

        dask.compute([c.filter(filename) for c in self.children])

    def set_leaves(self):
        if self.leaf:
            return self
        elif not self.empty:
            leaves = dask.compute([c.set_leaves() for c in self.children])
            # if returned from local dask, access first item in tuple
            if isinstance(leaves, tuple):
                leaves = leaves[0]
            # # higher level nodes
            if isinstance(leaves, list):
                if isinstance(leaves[0], list):
                    leaves = list(chain.from_iterable(leaves))
                self.leaves = [l for l in leaves if l.leaf]

            return self.leaves

    def find_dims(self, gs):
        rng = np.arange(1, gs+1, dtype=np.int32)
        factors = rng[np.where(gs % rng == 0)]
        idx = int((len(factors)/2)-1)
        x = factors[idx]
        y = int(gs / x)
        return [x, y]

    def get_read_chunks(self):
        res = self.parent_bounds.cell_size
        gs = self.parent_bounds.group_size
        x_size = self.maxx - self.minx
        y_size = self.maxy - self.miny

        xnum, ynum = self.find_dims(gs)

        dx = da.array([da.arange(x, x+xnum) for x in range(self.x1, self.x2, xnum)], dtype=np.int32)
        dy = da.array([da.arange(y, y+ynum) for y in range(self.y1, self.y2, ynum)], dtype=np.int32)
        print(dx.compute())







def create_bounds(reader, cell_size, group_size, polygon=None) -> Bounds:
    # grab our bounds
    if polygon:
        p = from_wkt(polygon)
        if not p.is_valid:
            raise Exception("Invalid polygon entered")

        b = p.bounds
        minx = b[0]
        miny = b[1]
        if len(b) == 4:
            maxx = b[2]
            maxy = b[3]
        elif len(b) == 6:
            maxx = b[3]
            maxy = b[4]
        else:
            raise Exception("Invalid bounds found.")

        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        srs = qi['srs']['wkt']
        if not srs:
            raise Exception("No SRS found in data.")

        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                         group_size=group_size, srs=srs)

        reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()

    else:
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        srs = qi['srs']['wkt']
        if not srs:
            raise Exception("No SRS found in data.")

        bbox = qi['bounds']
        minx = bbox['minx']
        maxx = bbox['maxx']
        miny = bbox['miny']
        maxy = bbox['maxy']
        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                    group_size=group_size, srs=srs)


    if not pc:
        raise Exception("No points found.")
    print("Points found",  pc)


    return bounds