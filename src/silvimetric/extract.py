import tiledb
import numpy as np
import dask.array as da
from osgeo import gdal

def extract(tdb_dir, out_dir, atts):
    # dd = da.from_tiledb(tdb_dir,'Z')
    # print(dd)
    with tiledb.open(tdb_dir, "r") as tdb:
        x1 = tdb.domain.dim("X")
        y1 = tdb.domain.dim("Y")
        shape = (int(y1.tile + 1),int(x1.tile + 1))
        q = tdb.query(attrs=atts)[:]
        xs = q['X']
        ys = q['Y']
        zs = q['Z']
        # hags = q['HeightAboveGround']
        # z_mean = np.array([z.mean() if z.any() else float('nan') for z in z_data], np.float64)
        # z_data = np.reshape(z_mean, newshape=(int(x.tile), int(y.tile)))
        z_data = np.empty(shape=shape, dtype=np.float64)
        z_data[:] = np.nan
        # hag_data = np.empty(shape=shape, dtype=np.float64)
        # hag_data[:] = np.nan
        # for x,y,z,hag in zip(xs,ys,zs,hags):

        for x,y,z in zip(xs,ys,zs):
            z_data[int(y),int(x)] = z.mean()
            # hag_data[int(y), int(x)] = hag.mean()

        # shape = (x.tile, y.tile)

        driver = gdal.GetDriverByName("GTiff")

        z_tif = driver.Create(f'{out_dir}/z_mean.tif', int(x1.tile + 1), int(y1.tile + 1), 1, gdal.GDT_Float64)
        z_tif.GetRasterBand(1).WriteArray(z_data)
        z_tif.GetRasterBand(1).SetNoDataValue(99999)
        z_tif.FlushCache()

        # hag_tif = driver.Create(f'{out_dir}/hag_nn_mean.tif', int(x1.tile + 1), int(y1.tile + 1), 1, gdal.GDT_Float32)
        # hag_tif.GetRasterBand(1).WriteArray(hag_data)
        # hag_tif.GetRasterBand(1).SetNoDataValue(99999)
        # hag_tif.FlushCache()


