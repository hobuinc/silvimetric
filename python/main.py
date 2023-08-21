import tiledb
import pdal
import math
import numpy as np
from dataclasses import dataclass
from dask.distributed import Client, progress
import time


#starting variables
# filename = "https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz"
filename = "autzen-classified.copc.laz"
cell_size = 100

class Bounds(object):
    def __init__(self, minx, miny, maxx, maxy, cell_size, srs=None):
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
        self.bounds = Bounds(minx, miny, maxx, maxy, parent.cell_size,
                            parent.srs);
        self.indices = [[i,j] for i in range(self.x1, self.x2+1)
                        for j in range(self.y1, self.y2+1)]


def arrange_data(point_data, bounds):

    points, chunk = point_data

    # Set up data object
    dx = []
    dy = []
    z_data = np.empty((0,0))
    count = np.array((0), np.int64)

    xis = np.floor((points['X'] - bounds.minx) / bounds.cell_size)
    yis = np.floor((points['Y'] - bounds.miny) / bounds.cell_size)

    for x, y in chunk.indices:

        if z_data.size == 0:
            add_data = np.array([points["Z"][np.logical_and(
                xis.astype(np.int32) == x, yis.astype(np.int32) == y)]],
                np.float64)
            if add_data.any():
                dx.append(x)
                dy.append(y)
                count = np.array([add_data.size], np.int64)
                z_data = add_data
        else:
            add_data = np.array([points["Z"][np.logical_and(
                xis.astype(np.int32) == x, yis.astype(np.int32) == y)]],
                np.float64)

            if add_data.any():
                dx.append(x)
                dy.append(y)
                count = np.append(count, add_data.size)
                cur_length = z_data.shape[1]
                if add_data.size < cur_length:
                    add_data = np.pad(add_data,
                        [(0,0), (0, z_data[0].size - add_data.size)],
                        mode='constant',
                        constant_values=float("nan"))
                elif add_data.size > cur_length:
                    z_data = np.pad(z_data,
                        [(0,0), (0, add_data.size - z_data[0].size)],
                        mode='constant',
                        constant_values=float("nan"))
                z_data = np.concatenate((z_data, add_data))

    # add last row so that tiledb doesn't throw us out
    # count = np.array([z.size for z in z_data], np.int64)
    dd = dict(count=count, Z=z_data)
    return [dx, dy, dd]

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)

    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = np.array(pipeline.arrays[0])

    return [points, chunk]

def create_tiledb(bounds: Bounds):
    if tiledb.object_type("stats") == "array":
        with tiledb.open("stats", "d") as A:
            A.query(cond="X>=0").submit()
    else:
        dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        z_att = tiledb.Attr(name="Z", dtype=np.float64, var=True)

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=bounds.xi*bounds.yi, attrs=[count_att, z_att])
        tiledb.SparseArray.create('stats', schema)

def create_bounds(pipeline) -> Bounds:
    # grab our bounds
    qi = pipeline.quickinfo['readers.copc']
    bbox = qi['bounds']
    minx = bbox['minx']
    maxx = bbox['maxx']
    miny = bbox['miny']
    maxy = bbox['maxy']
    srs = qi['srs']['wkt']

    return Bounds(minx, miny, maxx, maxy, cell_size=cell_size, srs=srs)

# tiledb errors if np array size is greater than array's first dim
# work around this by adding a None value to the array, which makes
# np not make it a multidim array
# https://github.com/TileDB-Inc/TileDB-Py/issues/494
def unpack_ndarrays(da, index_len: int):
    for arr in da:
        if da[arr].size > index_len:
            da[arr] = np.array([*da[arr], [None]], object)[:-1]
    return da

def main():
    start = time.time()

    # read pointcloud
    reader = pdal.Reader(filename)
    pipeline = reader.pipeline()
    bounds = create_bounds(pipeline)

    # set up tiledb
    create_tiledb(bounds)

    # write to tiledb
    with tiledb.SparseArray("stats", "w") as tdb:
        # apply metadata
        tdb.meta["LAYER_EXTENT_MINX"] = bounds.minx
        tdb.meta["LAYER_EXTENT_MINY"] = bounds.miny
        tdb.meta["LAYER_EXTENT_MAXX"] = bounds.maxx
        tdb.meta["LAYER_EXTENT_MAXY"] = bounds.maxy
        if (bounds.srs):
            tdb.meta["CRS"] = bounds.srs

        client = Client(threads_per_worker=12, n_workers=4,
            serializers=['dask', 'pickle'], deserializers=['dask', 'pickle'])


        # chunks = bounds.chunk()
        # chs = client.scatter(chunks)
        print("Reading and arranging chunks...")
        point_futures = [client.submit(get_data, reader=reader, chunk=ch) for ch in bounds.chunk()]
        data_futures = [client.submit(arrange_data, point_data=pf, bounds=bounds) for pf in point_futures]
        progress(*[data_futures, point_futures])

        for df in data_futures:
            dx, dy, dd = df.result()
            if len(dx):
                data = unpack_ndarrays(dd, len(dx))
                tdb[dx, dy] = data
        end = time.time()
        print("Done. Time elapsed: ", end-start)

if __name__ == "__main__":
    main()