from osgeo import gdal, osr
import numpy as np
from pathlib import Path

from .storage import Storage
from .config import ExtractConfiguration
from .metric import Metrics

np_to_gdal_types = {
    np.dtype(np.byte).str: gdal.GDT_Byte,
    np.dtype(np.int8).str: gdal.GDT_Int8,
    np.dtype(np.uint16).str: gdal.GDT_UInt16,
    np.dtype(np.int16).str: gdal.GDT_Int16,
    np.dtype(np.uint32).str: gdal.GDT_UInt32,
    np.dtype(np.int32).str: gdal.GDT_Int32,
    np.dtype(np.uint64).str: gdal.GDT_UInt64,
    np.dtype(np.int64).str: gdal.GDT_Int64,
    np.dtype(np.float32).str: gdal.GDT_Float32,
    np.dtype(np.float64).str: gdal.GDT_Float64
}

def write_tif(xsize: int, ysize: int, data:np.ndarray, name: str,
              config: ExtractConfiguration):
    osr.UseExceptions()
    path = Path(config.out_dir) / f'{name}.tif'
    crs = config.crs
    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.to_wkt())
    # transform = [x, res, 0, y, 0, res]
    b = config.bounds
    transform = [b.minx, config.resolution, 0,
                 b.maxy, 0, config.resolution]

    driver = gdal.GetDriverByName("GTiff")
    gdal_type = np_to_gdal_types[np.dtype(data.dtype).str]
    tif = driver.Create(str(path), int(xsize), int(ysize), 1, gdal_type)
    tif.SetGeoTransform(transform)
    tif.SetProjection(srs.ExportToWkt())
    tif.GetRasterBand(1).WriteArray(data)
    # tif.GetRasterBand(1).SetNoDataValue()
    tif.FlushCache()
    tif = None

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
            raster_data = np.empty(shape=(y1,x1), dtype=data[ma].dtype)
            raster_data.fill(None)
            m_data = data[['X','Y',ma]].to_numpy()
            for x,y,d in m_data:
                raster_data[int(y)][int(x)]=d
            write_tif(x1, y1, raster_data, ma, config)
