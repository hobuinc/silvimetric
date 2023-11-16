import math
import types
import json

import pdal
import numpy as np
from pyproj import CRS
from shapely import from_wkt

class Bounds(object):
    def __init__(self, minx: float, miny: float, maxx: float, maxy: float):
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy

    @staticmethod
    def from_string(bbox_str: str):
        #TODO accept more forms of bounds than just bbox array
        bbox = json.loads(bbox_str)
        if len(bbox) == 4:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[2]),
                            float(bbox[3]))
        elif len(bbox) == 6:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[3]),
                            float(bbox[4]))
        else:
            raise("Bounding boxes must have either 4 or 6 elements")

class Extents(object):

    #TODO take in bounds instead of minx,miny,maxx,maxy
    def __init__(self, bounds: Bounds, cell_size, group_size=16,
                 srs=None, is_chunk=False, root=None):


        self.is_chunk=is_chunk

        self.minx = float(bounds.minx)
        self.miny = float(bounds.miny)
        self.maxx = float(bounds.maxx)
        self.maxy = float(bounds.maxy)

        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        if self.srs.is_geographic:
            raise Exception(f"Extents SRS({srs}) is geographic.")
        self.epsg = self.srs.to_epsg()

        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.cell_size = cell_size
        self.group_size = group_size

        if is_chunk:
            if not root:
                raise("Chunked bounds are required to have a root")
            self.x1 = math.floor((self.minx - root.minx) / cell_size)
            self.y1 = math.floor((root.maxy - self.maxy) / cell_size)
            self.x2 = math.floor((self.maxx - root.minx) / cell_size)
            self.y2 = math.floor((root.maxy - self.miny) / cell_size)
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

        # buffers are only applied if the bounds do not fit on the cell line
        # x_buf = 1 if self.maxx % self.cell_size != 0 else 0
        # y_buf = 1 if self.maxy % self.cell_size != 0 else 0
        # make bounds in scale with the desired resolution
        minx = self.minx + (self.x1 * self.cell_size)
        maxx = self.minx + ((self.x2 + 1) * self.cell_size)
        miny = self.maxy - ((self.y2 + 1) * self.cell_size)
        maxy = self.maxy - (self.y1 * self.cell_size)

        chunk = Extents(minx, miny, maxx, maxy, self.cell_size, self.group_size,
                       self.srs.to_wkt(), is_chunk=True, root=self)
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
        midx = self.minx + ((self.maxx - self.minx)/ 2)
        midy = self.miny + ((self.maxy - self.miny)/ 2)
        yield from [
            Extents(self.minx, self.miny, midx, midy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #lower left
            Extents(midx, self.miny, self.maxx, midy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #lower right
            Extents(self.minx, midy, midx, self.maxy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root), #top left
            Extents(midx, midy, self.maxx, self.maxy, self.cell_size,
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
            Extents(minx, miny, maxx, maxy, self.cell_size,
                   self.group_size, self.srs.to_wkt(), True, self.root)
            for minx,maxx,miny,maxy in coords_list
        ]


    def __repr__(self):
        if self.srs:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}]) / EPSG:{self.epsg}"
        else:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"

def create_extents(reader, cell_size, group_size, polygon=None) -> Extents:
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

        bounds = Extents(minx, miny, maxx, maxy, cell_size=cell_size,
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
        bounds = Extents(minx, miny, maxx, maxy, cell_size=cell_size,
                    group_size=group_size, srs=srs)

    if not pc:
        raise Exception("No points found.")
    print("Points found",  pc)

    return bounds