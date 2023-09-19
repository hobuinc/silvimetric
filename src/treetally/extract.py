import tiledb
import numpy as np
import dask.array as da
from osgeo import gdal

def extract(tdb_dir, out_file):
    # dd = da.from_tiledb(tdb_dir,'Z')
    # print(dd)
    with tiledb.open(tdb_dir, "r") as tdb:
        x1 = tdb.domain.dim("X")
        y1 = tdb.domain.dim("Y")
        shape = (int(x1.tile),int(y1.tile))
        q = tdb.query(attrs=('Z'))[:]
        xs = q['X']
        ys = q['Y']
        zs = q['Z']
        # z_mean = np.array([z.mean() if z.any() else float('nan') for z in z_data], np.float64)
        # z_data = np.reshape(z_mean, newshape=(int(x.tile), int(y.tile)))
        z_data = np.empty(shape=shape, dtype=np.float64)
        z_data[:] = np.nan
        for x,y,z in zip(xs,ys,zs):
            z_data[int(x),int(y)] = z.mean()

        # shape = (x.tile, y.tile)

        driver = gdal.GetDriverByName("GTiff")

        outdata = driver.Create(out_file, int(y1.tile), int(x1.tile), 1, gdal.GDT_Float64)
        outdata.GetRasterBand(1).WriteArray(z_data)
        outdata.GetRasterBand(1).SetNoDataValue(99999)
        outdata.FlushCache()
