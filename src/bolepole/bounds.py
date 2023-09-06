import math

import numpy as np
from pyproj import CRS, Transformer

class Bounds(object):
    def __init__(self, minx, miny, maxx, maxy, cell_size, group_size = 3, srs=None):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)
        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        self.epsg = self.srs.to_epsg()


        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.xi = math.ceil(self.rangex / cell_size)
        self.yi = math.ceil(self.rangey / cell_size)
        self.cell_size = cell_size
        self.group_size = group_size

    def set_transform(self, src_srs):
        self.src_srs = CRS.from_user_input(src_srs)
        self.trn = Transformer.from_crs(self.src_srs, self.srs, always_xy=True)

    def transform(self, x_arr: np.ndarray, y_arr: np.ndarray):
        return self.trn.transform(x_arr, y_arr)

    def reproject(self):
        dst_crs = CRS.from_user_input(5070)
        trn = Transformer.from_crs(self.srs, dst_crs, always_xy=True)
        box = trn.transform([self.minx, self.maxx], [self.miny, self.maxy])
        self.__init__(box[0][0], box[1][0], box[0][1], box[1][1], self.cell_size, self.group_size, dst_crs)

    # since the box will always be a rectangle, chunk it by cell line?
    # return list of chunk objects to operate on
    def chunk(self):
        for i in range(0, self.xi):
            for j in range(0, self.yi, self.group_size):
                top = min(j+self.group_size-1, self.yi)
                yield Chunk([i,i], [j,top], self)

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
        self.x1, self.x2 = xrange
        self.y1 , self.y2 = yrange
        self.parent_bounds = parent
        cell_size = parent.cell_size
        group_size = parent.group_size
        self.srs = parent.srs
        minx = (self.x1 * cell_size) + parent.minx
        miny = (self.y1 * cell_size) + parent.miny
        maxx = (self.x2 + 1) * cell_size + parent.minx
        maxy = (self.y2 + 1) * cell_size + parent.miny
        self.bounds = Bounds(minx, miny, maxx, maxy, cell_size, group_size, self.srs.to_wkt())
        self.indices = [[i,j] for i in range(self.x1, self.x2+1)
                        for j in range(self.y1, self.y2+1)]