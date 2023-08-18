import tiledb
import pdal
import math
import numpy as np
from dataclasses import dataclass
from dask.distributed import Client, progress, as_completed



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


def get_data(reader, chunk):

    reader._options['bounds'] = str(chunk.bounds)

    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = pipeline.arrays[0]


    # Set up data object
    dx = []
    dy = []
    z_data = np.empty((0,0))
    # these need to match up in order to insert correctly
    shape_count = 0
    for i,j in chunk.indices:
        dx.append(i)
        dy.append(j)


    xis = np.floor((points['X'] - bounds.minx) / bounds.cell_size)
    yis = np.floor((points['Y'] - bounds.miny) / bounds.cell_size)
    for x, y in zip(dx, dy):
        if z_data.size == 0:
            z_data = np.array([points["Z"][np.logical_and(xis.astype(np.int32) == x, yis.astype(np.int32) == y)]], np.float64)
        else:
            add_data = np.array([points["Z"][np.logical_and(xis.astype(np.int32) == x, yis.astype(np.int32) == y)]], np.float64)
            cur_length = z_data.shape[1]
            if add_data.size < cur_length:
                add_data = np.pad(add_data, [(0,0), (0, z_data[0].size - add_data.size)], mode='constant', constant_values=float("nan"))
            elif add_data.size > cur_length:
                z_data = np.pad(z_data, [(0,0), (0, add_data.size - z_data[0].size)], mode='constant', constant_values=float("nan"))
            z_data = np.concatenate((z_data, add_data))
        # z_data.append(points["Z"][np.logical_and(xis.astype(np.int32) == x, yis.astype(np.int32) == y)])

    # add last row so that tiledb doesn't throw us out
    count = [z.size for z in z_data]
    # np_z = np.array([np.array(arr, dtype=np.float64) for arr in z_data], dtype=object)
    np_z = np.array([*z_data, None], dtype=object)
    # dd = { "count" : count, "Z": z_data[:-1] }
    dd = dict(count=count, Z=np_z[:-1])
    return [dx, dy, dd]

if __name__ == "__main__":
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
    count = qi['num_points']

    bounds = Bounds(minx, miny, maxx, maxy, cell_size=300, srs=srs)

    # set up tiledb
    dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
    dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
    domain = tiledb.Domain(dim_row, dim_col)

    count_att = tiledb.Attr(name="count", dtype=np.int32)
    z_att = tiledb.Attr(name="Z", dtype=np.float64, var=True)

    schema = tiledb.ArraySchema(domain=domain, sparse=True, capacity=bounds.xi*bounds.yi, attrs=[count_att, z_att])
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

        print("Reading chunks...")
        client = Client(threads_per_worker=12, n_workers=1)
        futures = []
        for chunk in bounds.chunk():
            f = client.submit(get_data, reader=reader, chunk=chunk )
            futures.append(f)
            progress(futures)
        client.gather(futures)
        for future in as_completed(futures):
            dx, dy, dd = future.result()
            tdb[dx, dy] = dd


