import tiledb
import numpy as np
import dask.array as da
from osgeo import gdal

from .storage import Storage
from .config import ExtractConfiguration
from .metric import Metric, Metrics

def write_tif(xsize, ysize, data, out_dir, name):
    driver = gdal.GetDriverByName("GTiff")

    tif = driver.Create(f'{out_dir}/{name}.tif', int(xsize + 1), int(ysize + 1), 1, gdal.GDT_Float64)
    tif.GetRasterBand(1).WriteArray(data)
    tif.GetRasterBand(1).SetNoDataValue(99999)
    tif.FlushCache()

def create_metric_att_list(metrics: list[str], attrs: list[str]):
    return [ Metrics[m].att(a) for m in metrics for a in attrs ]

def extract(config: ExtractConfiguration):
    ma_list = create_metric_att_list(config.metrics, config.attrs)
    storage = Storage.from_db(config.tdb_dir)
    with storage.open("r") as tdb:
        for ma in ma_list:
            tdb: tiledb.SparseArray
            x1 = tdb.domain.dim("X")
            y1 = tdb.domain.dim("Y")
            # shape = (int(y1.tile + 1),int(x1.tile + 1))
            write_tif(x1.tile, y1.tile, tdb[:][ma], config.out_dir, ma)
