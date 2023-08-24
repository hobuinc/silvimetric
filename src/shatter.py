import time
import math
import argparse
from os import environ

import tiledb
import pdal
import numpy as np
import shapely
from pyproj import CRS, Transformer, network

from dask.distributed import Client, progress, performance_report
from distributed.diagnostics import MemorySampler

environ['PROJ_NETWORK'] = 'OFF'
network.set_network_enabled(False)


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


def arrange_data(point_data, bounds: Bounds):

    points: np.ndarray
    chunk: Chunk

    points, chunk = point_data
    src_crs = bounds.src_srs
    if src_crs != chunk.srs:
        x_points, y_points = bounds.transform(points['X'], points['Y'])
        xis = np.floor((x_points - bounds.minx) / bounds.cell_size)
        yis = np.floor((y_points - bounds.miny) / bounds.cell_size)
    else:
        xis = np.floor((points['X'] - bounds.minx) / bounds.cell_size)
        yis = np.floor((points['Y'] - bounds.miny) / bounds.cell_size)

    # Set up data object
    dx = []
    dy = []
    count = np.array([], dtype=np.int32)
    z_data = np.zeros((points.size), dtype=np.float64)

    offset = 0
    offsets = []
    for x, y in chunk.indices:
        add_data = np.array([points["Z"][np.logical_and(
            xis.astype(np.int32) == x, yis.astype(np.int32) == y)]],
            np.float64, copy=False)

        if add_data.any():
            dx.append(x)
            dy.append(y)
            size = add_data.size

            count = np.append(count, size)

            n = offset + size
            z_data[offset:n] = add_data
            offsets.append([offset, n])
            offset = n

    r = np.array([z_data[lo:hi] for lo,hi in offsets], dtype=object)
    if r.size == count.size:
        dd = dict(count=count, Z=r)
    else:
        dd = dict(count=count, Z=np.array([r, None], object)[:-1])
    # Tiledb errors if np array size is greater than array's first dim.
    # Will work around this by adding a None value to the array, which forces
    # numpy to accept it as a 1D object
    # https://github.com/TileDB-Inc/TileDB-Py/issues/494
    return [dx, dy, dd]

def get_data(reader, chunk):
    reader._options['bounds'] = str(chunk.bounds)

    # remember that readers.copc is a thread hog
    reader._options['threads'] = 1
    pipeline = reader.pipeline()
    pipeline.execute()
    points = np.array(pipeline.arrays[0], copy=False)
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

def create_bounds(reader, cell_size, group_size, polygon=None, p_srs=None) -> Bounds:
    # grab our bounds
    if polygon:
        p = shapely.from_wkt(polygon)
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

        # TODO handle srs that's geographic
        user_crs = CRS.from_user_input(p_srs)
        user_wkt = user_crs.to_wkt()

        bounds = Bounds(minx, miny, maxx, maxy, cell_size, group_size, user_wkt)
        bounds.reproject()

        reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()

        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        src_srs = qi['srs']['wkt']
        bounds.set_transform(src_srs)

        if not pc:
            raise Exception("No points found.")

        return bounds
    else:

        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]

        if not qi['num_points']:
            raise Exception("No points found.")

        bbox = qi['bounds']
        minx = bbox['minx']
        maxx = bbox['maxx']
        miny = bbox['miny']
        maxy = bbox['maxy']
        srs = qi['srs']['wkt']
        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                    group_size=group_size, srs=srs)
        bounds.set_transform(srs)

        return bounds

def write_tdb(tdb, res):
    dx, dy, dd = res
    tdb[dx, dy] = dd
    return dd['count'].sum()

def get_time_string(start, end):
    final_time_s = (end-start)/pow(10,9)

    hrs = final_time_s / 60 / 60
    hrs_flr = math.floor(hrs)

    mins = (hrs - hrs_flr) * 60
    mins_flr = math.floor(mins)

    secs = (mins - mins_flr) * 60
    return f"{hrs_flr}:{mins_flr}:{secs}"

def main(filename: str, threads: int, workers: int, group_size: int, res: float,
         no_threads: bool, stats_bool: bool, polygon=None, p_srs=None):


    # read pointcloud
    reader = pdal.Reader(filename)
    bounds = create_bounds(reader, res, group_size, polygon, p_srs)

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
            tdb.meta["CRS"] = bounds.srs.to_wkt()

        if stats_bool:
            pc = 0
            cell_count = bounds.xi * bounds.yi
            chunk_count = 0
            start = time.perf_counter_ns()

        if no_threads:
            it = bounds.chunk()
            for ch in it:
                point_data = get_data(reader=reader, chunk=ch)
                arranged_data = arrange_data(point_data=point_data, bounds=bounds)
                point_count = write_tdb(tdb, arranged_data)

                if stats_bool:
                    pc += point_count
                    chunk_count += 1

        else:
            # configure diagnostic pieces
            client = Client(threads_per_worker=threads, n_workers=workers,
                serializers=['dask', 'pickle'])
            b = (client.scatter(bounds, broadcast=True)).result()

            with performance_report():
                point_futures = [client.submit(get_data, reader=reader, chunk=ch)
                                for ch in b.chunk()]
                data_futures = [client.submit(arrange_data, point_data=pf, bounds=b)
                                for pf in point_futures]
                write_futures = [client.submit(write_tdb, tdb=tdb, res=df)
                                for df in data_futures]
                client.gather([point_futures, data_futures, write_futures])

            if stats_bool:
                pc = 0
                chunk_count = len(data_futures)
                for wf in write_futures:
                    pc += wf.result()

        if stats_bool:
            end = time.perf_counter_ns()
            t = get_time_string(start,end)

            stats = dict(
                time=t, threads=threads, workers=workers,
                group_size=group_size, resolution=res, point_count=pc,
                cell_count=cell_count, chunk_count=chunk_count,
                filename=filename
            )
            print(stats)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--group_size", type=int, default=3)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--polygon_srs", type=str, default="EPSG:4326")
    parser.add_argument("--no_threads", type=bool, default=False)
    parser.add_argument("--stats", type=bool, default=False)

    args = parser.parse_args()

    filename = args.filename
    threads = args.threads
    workers = args.workers
    group_size = args.group_size
    res = args.resolution
    poly = args.polygon
    p_srs = args.polygon_srs

    no_threads = args.no_threads
    stats_bool = args.stats


    main(filename, threads, workers, group_size, res, no_threads, stats_bool,
          poly, p_srs)