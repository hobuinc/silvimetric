from osgeo import gdal, osr
import numpy as np
from pathlib import Path

from ..resources import Storage, ExtractConfig, Metric, Attribute
from ..resources import Extents

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
              config: ExtractConfig):
    osr.UseExceptions()
    path = Path(config.out_dir) / f'{name}.tif'
    crs = config.crs
    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.to_wkt())
    # transform = [x, res, 0, y, 0, res]
    b = config.bounds

    transform = [b.minx, config.resolution, 0,
                 b.maxy, 0, -1* config.resolution]

    driver = gdal.GetDriverByName("GTiff")
    gdal_type = np_to_gdal_types[np.dtype(data.dtype).str]
    tif = driver.Create(str(path), int(xsize), int(ysize), 1, gdal_type)
    tif.SetGeoTransform(transform)
    tif.SetProjection(srs.ExportToWkt())
    tif.GetRasterBand(1).WriteArray(data)
    tif.GetRasterBand(1).SetNoDataValue(np.nan)
    tif.FlushCache()
    tif = None

def create_metric_att_list(metrics: list[Metric], attrs: list[Attribute]):
    return [ m.entry_name(a.name) for m in metrics for a in attrs ]

def extract(config: ExtractConfig):

    ma_list = create_metric_att_list(config.metrics, config.attrs)
    storage = Storage.from_db(config.tdb_dir)
    root_bounds=storage.config.bounds

    e = Extents(config.bounds, config.resolution, root=root_bounds)
    i = e.indices
    minx = i['x'].min()
    maxx = i['x'].max()
    miny = i['y'].min()
    maxy = i['y'].max()
    x1 = maxx - minx + 1
    y1 = maxy - miny + 1

    with storage.open("r") as tdb:
        data = tdb.query(attrs=ma_list, order='F', coords=True).df[minx:maxx, miny:maxy]
        data['X'] = data['X'] - minx
        data['Y'] = data['Y'] - miny

        for ma in ma_list:
            m_data = np.full(shape=(y1,x1), fill_value=np.nan, dtype=data[ma].dtype)
            a = data[['X','Y',ma]].to_numpy()
            for x,y,md in a[:]:
                m_data[int(y)][int(x)] = md

            write_tif(x1, y1, m_data, ma, config)
