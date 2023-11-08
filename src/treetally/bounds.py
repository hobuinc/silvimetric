import math
import types

import pdal
import numpy as np
from pyproj import CRS
from shapely import from_wkt

class Bounds(object):

    def __init__(self, minx, miny, maxx, maxy, cell_size, group_size=16,
                 srs=None, is_chunk=False, root=None):

        self.is_chunk=is_chunk

        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)
        self.midx = self.minx + ((self.maxx - self.minx)/ 2)
        self.midy = self.miny + ((self.maxy - self.miny)/ 2)

        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        if self.srs.is_geographic:
            raise Exception(f"Bounds SRS({srs}) is geographic.")
        self.epsg = self.srs.to_epsg()

        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.cell_size = cell_size
        self.group_size = group_size

        if is_chunk:
            if not root:
                raise("Chunked bounds are required to have a root")
            self.x1 = math.floor((minx - root.minx) / cell_size)
            self.y1 = math.floor((root.maxy - maxy) / cell_size)
            self.x2 = math.floor((maxx - root.minx) / cell_size)
            self.y2 = math.floor((root.maxy - miny) / cell_size)
            self.root = root
            self.indices = np.array(
                [(i,j) for i in range(self.x1, self.x2)
                for j in range(self.y1, self.y2)],
                dtype=[('x', np.int32), ('y', np.int32)]
            )

        else:
            self.root = self
            self.x1 = 0
            self.x2 = math.floor(self.rangex / cell_size)
            self.y1 = 0
            self.y2 = math.floor(self.rangey / cell_size)
            self.indices = np.array(
                [(i,j) for i in range(self.x1, self.x2 + 1)
                for j in range(self.y1, self.y2 + 1)],
                dtype=[('x', np.int32), ('y', np.int32)]
            )


    def __eq__(self, other):
        if self.minx != other.minx:
            return False
        if self.maxx != other.maxx:
            return False
        if self.miny != other.miny:
            return False
        if self.maxy != other.maxy:
            return False
        if self.srs != other.srs:
            return False
        return True

    def chunk(self, filename:str, threshold=1000) :
        if self.is_chunk:
            raise Exception("Cannot perform chunk on a previously chunked bounds")
        self.is_chunk = True

        # make bounds in scale with the desired resolution
        minx = self.minx + (self.x1 * self.cell_size)
        maxx = self.minx + ((self.x2 + 1) * self.cell_size)
        miny = self.maxy - ((self.y2 + 1) * self.cell_size)
        maxy = self.maxy - (self.y1 * self.cell_size)

        chunk = Bounds(minx, miny, maxx, maxy, self.cell_size, self.group_size,
                       self.srs.to_wkt(), is_chunk=True, root=self)
        self.root_chunk: Bounds = chunk

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
                    elif isinstance(n, Bounds):
                        l.append(n)
                except StopIteration:
                    return l

        leaves: list[Bounds] = get_leaves(filtered)
        yield from [bounds for leaf in leaves for bounds in leaf.get_leaf_children()]

    def split(self):
        yield from [
            Bounds(self.minx, self.miny, self.midx, self.midy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #lower left
            Bounds(self.midx, self.miny, self.maxx, self.midy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #lower right
            Bounds(self.minx, self.midy, self.midx, self.maxy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #top left
            Bounds(self.midx, self.midy, self.maxx, self.maxy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root)  #top right
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

    def find_dims(self):
        gs = self.group_size
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
        res = self.cell_size
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
            Bounds(minx, miny, maxx, maxy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root)
            for minx,maxx,miny,maxy in coords_list
        ]


    def __repr__(self):
        if self.srs:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}]) / EPSG:{self.epsg}"
        else:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"

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