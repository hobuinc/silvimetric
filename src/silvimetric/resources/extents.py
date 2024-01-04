import math
import numpy as np
import dask
import dask.bag as db

from .bounds import Bounds
from .storage import Storage
from .data import Data

class Extents(object):

    def __init__(self,
                 bounds: Bounds,
                 resolution: float,
                 root: Bounds, tile_size: int=16):

        self.bounds = bounds
        self.root = root

        minx, miny, maxx, maxy = self.bounds.get()

        self.rangex = maxx - minx
        self.rangey = maxy - miny
        self.resolution = resolution
        self.tile_size = tile_size

        self.x1 = math.floor((minx - self.root.minx) / resolution)
        self.y1 = math.floor((self.root.maxy - maxy) / resolution)
        self.x2 = math.floor((maxx - self.root.minx) / resolution)
        self.y2 = math.floor((self.root.maxy - miny) / resolution)
        self.indices = np.array(
            [(i,j) for i in range(self.x1, self.x2)
            for j in range(self.y1, self.y2)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )
        self.leaf = False

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

        chunk = Extents(Bounds(minx, miny, maxx, maxy), self.resolution, r, self.tile_size)
        self.root_chunk: Extents = chunk

        filtered = []
        curr = db.from_delayed(self.filter(data, threshold))
        while True:
            vals = curr.compute()
            nxt = [one for one in vals if not isinstance(one, Extents)]
            print(any([isinstance(x, Extents) for x in nxt]))

            to_add = [one for one in vals if isinstance(one, Extents)]
            if to_add:
                filtered.append(to_add)

            curr = db.from_delayed(nxt)




        return db.from_sequence(filtered)

        # filtered = chunk.filter(data, threshold)
        # def flatten(il):
        #     ol = []
        #     for s in il:
        #         if isinstance(s, list):
        #             ol.append(flatten(il))
        #         ol.append(s)
        #     return ol

        # def get_leaves(c):
        #     l = []
        #     while True:
        #         try:
        #             n = next(c)
        #             if isinstance(n, types.GeneratorType):
        #                 l += flatten(get_leaves(n))
        #             elif isinstance(n, Extents):
        #                 l.append(n)
        #         except StopIteration:
        #             return l

        # leaves: list[Extents] = get_leaves(filtered)
        # yield from [bounds for leaf in leaves for bounds in leaf.get_leaf_children()]

    def split(self):
        minx, miny, maxx, maxy = self.bounds.get()
        midx = minx + ((maxx - minx)/ 2)
        midy = miny + ((maxy - miny)/ 2)
        return [
            Extents(Bounds(minx, miny, midx, midy), self.resolution,
                    self.root, self.tile_size), #lower left
            Extents(Bounds(midx, miny, maxx, midy), self.resolution,
                    self.root, self.tile_size), #lower right
            Extents(Bounds(minx, midy, midx, maxy), self.resolution,
                    self.root, self.tile_size), #top left
            Extents(Bounds(midx, midy, maxx, maxy), self.resolution,
                    self.root, self.tile_size)  #top right
        ]

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    @dask.delayed
    def filter(self, data: Data, threshold_resolution=100):
        if self.leaf:
            return self

        pc = data.estimate_count(self.bounds)
        # pc = qi['num_points']
        minx, miny, maxx, maxy = self.bounds.get()

        # is it empty?
        if not pc:
            return [ ]
        else:
            # has it hit the threshold yet?
            area = (maxx - minx) * (maxy - miny)
            if maxx - minx < threshold_resolution:
                self.leaf = True
                return self
            else:
                return [ ch.filter(data, threshold_resolution) for ch in self.split() ]

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
                   self.root, self.tile_size)
            for minx,maxx,miny,maxy in coords_list
        ]

    @staticmethod
    def from_storage(tdb_dir: str, tile_size: float=16):
        storage = Storage.from_db(tdb_dir)
        meta = storage.getConfig()
        return Extents(meta.root, meta.resolution, meta.root, tile_size)

    @staticmethod
    def from_sub(tdb_dir: str, sub: Bounds, tile_size: float=16):

        storage = Storage.from_db(tdb_dir)

        meta = storage.getConfig()
        res = meta.resolution
        base_extents = Extents(meta.root, res, meta.root, tile_size)
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
        return Extents(new_b, res, base, tile_size)


    # @staticmethod
    # def create(reader, resolution: float=30, tile_size: float=16, polygon=None):
    #     # grab our bounds
    #     if polygon is not None:
    #         p = from_wkt(polygon)
    #         if not p.is_valid:
    #             raise Exception("Invalid polygon entered")

    #         b = p.bounds
    #         minx = b[0]
    #         miny = b[1]
    #         if len(b) == 4:
    #             maxx = b[2]
    #             maxy = b[3]
    #         elif len(b) == 6:
    #             maxx = b[3]
    #             maxy = b[4]
    #         else:
    #             raise Exception("Invalid bounds found.")

    #         qi = pipeline.quickinfo[reader.type]
    #         pc = qi['num_points']
    #         if not pc:
    #             raise Exception("No points found.")

    #         extents = Extents(Bounds(minx, miny, maxx, maxy), resolution,
    #                         tile_size)

    #         reader._options['bounds'] = str(extents)
    #         pipeline = reader.pipeline()

    #     else:
    #         pipeline = reader.pipeline()
    #         qi = pipeline.quickinfo[reader.type]
    #         pc = qi['num_points']
    #         if not pc:
    #             raise Exception("No points found.")

    #         bbox = qi['bounds']
    #         minx = bbox['minx']
    #         maxx = bbox['maxx']
    #         miny = bbox['miny']
    #         maxy = bbox['maxy']
    #         extents = Extents(Bounds(minx, miny, maxx, maxy), resolution,
    #                     tile_size)


        return extents


    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds.get()
        return f"([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}])"
