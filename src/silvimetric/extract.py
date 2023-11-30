from osgeo import gdal

from .storage import Storage
from .config import ExtractConfiguration
from .metric import Metrics

def write_tif(xsize, ysize, data, out_dir, name):
    driver = gdal.GetDriverByName("GTiff")

    tif = driver.Create(f'{out_dir}/{name}.tif', int(xsize), int(ysize), 1, gdal.GDT_Float64)
    tif.GetRasterBand(1).WriteArray(data)
    tif.GetRasterBand(1).SetNoDataValue(99999)
    tif.FlushCache()

def create_metric_att_list(metrics: list[str], attrs: list[str]):
    return [ Metrics[m].att(a) for m in metrics for a in attrs ]

def extract(config: ExtractConfiguration):
    ma_list = create_metric_att_list(config.metrics, config.attrs)
    storage = Storage.from_db(config.tdb_dir)
    with storage.open("r") as tdb:
        data = tdb.query(attrs=ma_list, coords=True).df[:].sort_values(['Y','X'])
        x1 = tdb.domain.dim("X").tile
        y1 = tdb.domain.dim("Y").tile

        for ma in ma_list:
            d = data[ma].to_numpy().reshape((x1, y1))
            write_tif(x1, y1, d, config.out_dir, ma)
