import tiledb
import numpy as np
import dask.array as da
from osgeo import gdal

from .storage import Storage
from .config import ExtractConfiguration

def write_tif(xsize, ysize, data, out_dir, name):
    driver = gdal.GetDriverByName("GTiff")

    tif = driver.Create(f'{out_dir}/{name}.tif', int(xsize + 1), int(ysize + 1), 1, gdal.GDT_Float64)
    tif.GetRasterBand(1).WriteArray(data)
    tif.GetRasterBand(1).SetNoDataValue(99999)
    tif.FlushCache()

def extract(config: ExtractConfiguration):
    storage = Storage.from_db(config.tdb_dir)
    with storage.open("r") as tdb:
        tdb: tiledb.SparseArray
        x1 = tdb.domain.dim("X")
        y1 = tdb.domain.dim("Y")
        shape = (int(y1.tile + 1),int(x1.tile + 1))
        att_data = tdb.query(attrs=config.attrs, coords=False)[:]

        for att in att_data:
            write_tif(x1.tile, y1.tile, att_data[att], config.out_dir)

        # xs = q['X']
        # ys = q['Y']
        # atts = [a for a in config.attrs]
        # att_data = q['Z']

        # z_data = np.empty(shape=shape, dtype=np.float64)
        # z_data[:] = np.nan

        #TODO flatten on axis 1? and do mean there. See if it means all cells
        # for x,y,z in zip(xs,ys,att_data):
        #     z_data[int(y),int(x)] = z.flatten().mean()
