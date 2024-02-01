from osgeo import gdal, osr
import numpy as np
from pathlib import Path
import pandas as pd
import itertools

from ..resources import Storage, ExtractConfig, Metric, Attribute
from ..resources import Extents
from ..commands.shatter import get_metrics

np_to_gdal_types = {
    np.dtype(np.byte).str: gdal.GDT_Byte,
    np.dtype(np.int8).str: gdal.GDT_Int16,
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

def extract(config: ExtractConfig):

    storage = Storage.from_db(config.tdb_dir)
    ma_list = [ m.entry_name(a.name) for m in config.metrics for a in
        config.attrs ]
    att_list = [ a.name for a in config.attrs ] + [ 'count' ]
    out_list = [ *ma_list, 'X', 'Y' ]
    root_bounds=storage.config.root

    e = Extents(config.bounds, config.resolution, root=root_bounds)
    i = e.get_indices()
    minx = i['x'].min()
    maxx = i['x'].max()
    miny = i['y'].min()
    maxy = i['y'].max()
    x1 = maxx - minx + 1
    y1 = maxy - miny + 1

    with storage.open("r") as tdb:
        data = tdb.query(attrs=ma_list, order='F', coords=True).df[minx:maxx,
                miny:maxy]
        data['X'] = data['X'] - minx
        data['Y'] = data['Y'] - miny

        # 1. should find values that are not unique, meaning they have multiple
        # entries
        recarray = data.to_records()
        xys = recarray[['X', 'Y']]
        unq, idx, inv, counts = np.unique(xys, return_index=True, return_inverse=True, return_counts=True)
        redos = np.where(counts >= 2)
        leaves = data.loc[idx[np.where(counts == 1)]]

        # 2. then should combine the values of those attribute/cell combos
        ridx = np.unique(unq[redos])
        rxs = list(ridx['X'])
        rys = list(ridx['Y'])
        redo = pd.DataFrame(tdb.query(attrs=att_list).multi_index[[rxs], [rys]])
        recs = redo.groupby(['X','Y'], as_index=False).agg(
            lambda x: np.fromiter(itertools.chain(*x), x.dtype)
            if x.dtype == object else sum(x))[[*att_list, 'X', 'Y']]
        recs = recs.to_dict(orient='list')

        # 3. Should rerun the metrics over them
        dx, dy, metrics = get_metrics([[],[],recs], att_list, storage)
        ms = pd.DataFrame.from_dict(metrics)[out_list]
        final = pd.concat((ms, leaves[out_list]))

        # 4. output them to tifs
        xs = final['X'].max() + 1
        ys = final['Y'].max() + 1
        for ma in ma_list:
            raster_data = np.full((ys, xs), np.nan)
            raster_idx = final['Y']*ys + final['X']
            np.put(raster_data, raster_idx, final[ma])
            write_tif(x1, y1, raster_data, ma, config)