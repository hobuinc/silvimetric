import tiledb
import pdal
import math
import numpy as np
from dataclasses import dataclass


#starting variables
# filename = "https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz"
filename = "autzen-classified.copc.laz"
cell_size = 300

# TODO read autzen
reader = pdal.Reader(filename)
pipeline = reader.pipeline()

# grab our bounds
qi = pipeline.quickinfo['readers.copc']
bbox = qi['bounds']
minx = bbox['minx']
maxx = bbox['maxx']
miny = bbox['miny']
maxy = bbox['maxy']
srs = qi['srs']['wkt']

class Bounds(object):
    def __init__(self, minx, miny, maxx, maxy, cell_size = 300, srs=None):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)
        self.srs = srs
        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.xi = math.ceil(self.rangex / cell_size)
        self.yi = math.ceil(self.rangey / cell_size)
        self.cell_size = cell_size
        self.group_size = 3

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

    def get_cell(self, x, y):
        xcell = math.floor((x - self.minx) / self.cell_size)
        ycell = math.floor((y - self.miny) / self.cell_size)
        return [xcell, ycell]

    def cell_dim(self, x, y):
        b = self.split(x, y)
        xcenter = (b.maxx - b.minx) / 2
        ycenter = (b.maxy - b.miny) / 2
        return [xcenter, ycenter]


    def __repr__(self):
        return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"

class Chunk(object):
    def __init__(self, xrange: list[int], yrange: list[int], parent: Bounds):
        self.x1, self.x2 = xrange
        self.y1 , self.y2 = yrange
        self.parent_bounds = parent
        minx = (self.x1 * cell_size) + parent.minx
        miny = (self.y1 * cell_size) + parent.miny
        maxx = (self.x2 + 1) * cell_size + parent.minx
        maxy = (self.y2 + 1) * cell_size + parent.miny
        self.bounds = Bounds(minx, miny, maxx, maxy, parent.cell_size, parent.srs);
        self.indices = [[i,j] for i in range(self.x1, self.x2+1) for j in range(self.y1, self.y2+1)]

bounds = Bounds(minx, miny, maxx, maxy, cell_size=300, srs=srs)

def get_counts(data, indices):

    # Set up data object
    dx = []
    dy = []
    dd = []
    # these need to match up in order to insert correctly
    for i,j in indices:
        dd.append({"count": 0})
        dx.append(i)
        dy.append(j)

    # collect data and insert into data obj
    for point in data:
        xi, yi = bounds.get_cell(point['X'], point['Y'])
        for it in range(len(dx)):
            if (xi == dx[it] and yi == dy[it]):
                dd[it]["count"] += 1

    return [dx, dy, dd]

def get_stats(reader, chunk):

    reader._options['bounds'] = str(chunk.bounds)

    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = pipeline.arrays[0]
    return get_counts(points, chunk.indices)

# set up tiledb
dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
domain = tiledb.Domain(dim_row, dim_col)
count_att = tiledb.Attr(name="count", dtype=np.int32)

schema = tiledb.ArraySchema(domain=domain, sparse=True, attrs=[count_att])
tdb = tiledb.SparseArray.create('stats', schema)


# TODO write to tiledb
with tiledb.SparseArray("stats", "w") as tdb:
    # apply metadata
    tdb.meta["LAYER_EXTENT_MINX"] = bounds.minx
    tdb.meta["LAYER_EXTENT_MINY"] = bounds.miny
    tdb.meta["LAYER_EXTENT_MAXX"] = bounds.maxx
    tdb.meta["LAYER_EXTENT_MAXY"] = bounds.maxy
    if (bounds.srs):
        tdb.meta["CRS"] = bounds.srs

    gs = bounds.group_size
    print("Reading chunks...")
    for chunk in bounds.chunk():
        dx, dy, dd = get_stats(reader=reader, chunk=chunk)
        for i in range(len(dx)):
            tdb[dx[i], dy[i]] = dd[i]
        print("Chunk: (", dx, ", ", dy, ") processed.")

