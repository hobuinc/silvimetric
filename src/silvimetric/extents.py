import math
import types

import pdal
import numpy as np
from pyproj import CRS
from shapely import from_wkt

from .bounds import Bounds
from .storage import Storage

class Extents(object):

    def __init__(self, bounds: Bounds, resolution: float, tile_size: int=16,
                 srs: str=None, root: Bounds=None):

        self.bounds = bounds
        if root is None:
            root = bounds
        self.root = root
        minx, miny, maxx, maxy = self.bounds.get()

        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        if self.srs.is_geographic:
            raise Exception(f"Extents SRS({srs}) is geographic.")
        self.epsg = self.srs.to_epsg()

        self.rangex = maxx - minx
        self.rangey = maxy - miny
        self.resolution = resolution
        self.tile_size = tile_size

        self.x1 = math.floor((minx - root.minx) / resolution)
        self.y1 = math.floor((root.maxy - maxy) / resolution)
        self.x2 = math.floor((maxx - root.minx) / resolution)
        self.y2 = math.floor((root.maxy - miny) / resolution)
        self.indices = np.array(
            [(i,j) for i in range(self.x1, self.x2)
            for j in range(self.y1, self.y2)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )

    def chunk(self, filename:str, threshold=1000) :
        bminx, bminy, bmaxx, bmaxy = self.bounds.get()

        # buffers are only applied if the bounds do not fit on the cell line
        # in which case we're extending the bounds to the next nearest cell line
        x_buf = 1 if bmaxx % self.resolution != 0 else 0
        y_buf = 1 if bmaxy % self.resolution != 0 else 0
        # make bounds in scale with the desired resolution
        minx = bminx + (self.x1 * self.resolution)
        maxx = bminx + ((self.x2 + x_buf) * self.resolution)
        miny = bmaxy - ((self.y2 + y_buf) * self.resolution)
        maxy = bmaxy - (self.y1 * self.resolution)

        chunk = Extents(Bounds(minx, miny, maxx, maxy), self.resolution, self.tile_size,
                       self.srs.to_wkt(), root=self.bounds)
        self.root_chunk: Extents = chunk

        filtered = chunk.filter(filename, threshold)

        def flatten(il):
            ol = []
            for s in il:
                if isinstance(s, list):
                    ol.append(flatten(il))
                ol.append(s)
            return ol

        def get_leaves(c):
            l = []
            while True:
                try:
                    n = next(c)
                    if isinstance(n, types.GeneratorType):
                        l += flatten(get_leaves(n))
                    elif isinstance(n, Extents):
                        l.append(n)
                except StopIteration:
                    return l

        leaves: list[Extents] = get_leaves(filtered)
        yield from [bounds for leaf in leaves for bounds in leaf.get_leaf_children()]

    def split(self):
        minx, miny, maxx, maxy = self.bounds.get()
        midx = minx + ((maxx - minx)/ 2)
        midy = miny + ((maxy - miny)/ 2)
        yield from [
            Extents(Bounds(minx, miny, midx, midy), self.resolution,
                    self.tile_size, self.srs.to_wkt(), self.root), #lower left
            Extents(Bounds(midx, miny, maxx, midy), self.resolution,
                   self.tile_size, self.srs.to_wkt(), self.root), #lower right
            Extents(Bounds(minx, midy, midx, maxy), self.resolution,
                   self.tile_size, self.srs.to_wkt(), self.root), #top left
            Extents(Bounds(midx, midy, maxx, maxy), self.resolution,
                   self.tile_size, self.srs.to_wkt(), self.root)  #top right
        ]

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    def filter(self, filename, threshold=1000):
        reader: pdal.Reader = pdal.Reader(filename)
        reader._options['bounds'] = str(self)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        minx, miny, maxx, maxy = self.bounds.get()

        # is it empty?
        if not pc:
            yield None
        else:
            # has it hit the threshold yet?
            area = (maxx - minx) * (maxy - miny)
            t = threshold**2
            if area < t:
                yield self
            else:
                children = self.split()
                yield from [c.filter(filename,threshold) for c in children]

    def find_dims(self):
        gs = self.tile_size
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
        res = self.resolution
        xnum, ynum = self.find_dims()

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
            Extents(Bounds(minx, miny, maxx, maxy), self.resolution,
                   self.tile_size, self.srs.to_wkt(), self.root)
            for minx,maxx,miny,maxy in coords_list
        ]

    @staticmethod
    def from_storage(storage: Storage, tile_size: float=16):
        meta = storage.getConfig()
        return Extents(meta.bounds, meta.resolution, tile_size, meta.crs)

    @staticmethod
    def create(reader, resolution: float=30, tile_size: float=16, polygon=None):
        # grab our bounds
        if polygon is not None:
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
            if not pc:
                raise Exception("No points found.")
            srs = qi['srs']['wkt']
            if not srs:
                raise Exception("No SRS found in data.")

            extents = Extents(Bounds(minx, miny, maxx, maxy), resolution,
                            tile_size, srs)

            reader._options['bounds'] = str(extents)
            pipeline = reader.pipeline()

        else:
            pipeline = reader.pipeline()
            qi = pipeline.quickinfo[reader.type]
            pc = qi['num_points']
            if not pc:
                raise Exception("No points found.")
            srs = qi['srs']['wkt']
            if not srs:
                raise Exception("No SRS found in data.")

            bbox = qi['bounds']
            minx = bbox['minx']
            maxx = bbox['maxx']
            miny = bbox['miny']
            maxy = bbox['maxy']
            extents = Extents(Bounds(minx, miny, maxx, maxy), resolution,
                        tile_size, srs)


        return extents


    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds.get()
        # if self.srs:
        #     return f"([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}]) / EPSG:{self.epsg}"
        # else:
        return f"([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}])"