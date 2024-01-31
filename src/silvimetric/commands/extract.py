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

def create_metric_att_list(metrics: list[Metric], attrs: list[Attribute]):
    return [ m.entry_name(a.name) for m in metrics for a in attrs ]

def extract(config: ExtractConfig):

    storage = Storage.from_db(config.tdb_dir)
    ma_list = create_metric_att_list(config.metrics, config.attrs)
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
        att_list = [a.name for a in config.attrs] + ['count']
        #TODO adjust the other data references to account for new query
        data = tdb.query(attrs=[*ma_list, *att_list], use_arrow=False, order='F',
                coords=True).df[minx:maxx, miny:maxy]
        data['X'] = data['X'] - minx
        data['Y'] = data['Y'] - miny

        # 1. should find values that are not unique, meaning they have multiple
        # entries
        recarray = data.to_records()
        xys = recarray[['X', 'Y']]
        unq, idx, counts = np.unique(xys, return_index=True, return_counts=True)
        redos = data.loc[idx[np.where(counts >= 2)]]
        leaves = data.loc[idx[np.where(counts == 1)]]

        # 2. then should combine the values of those attribute/cell combos
        # ridx = np.unique(unq[redos])
        # rxs = list(ridx['X'])
        # rys = list(ridx['Y'])
        # redo = pd.DataFrame(tdb.query(attrs=att_list).multi_index[[rxs], [rys]])
        recs = redos.groupby(['X','Y'], as_index=False).agg(
            lambda x: np.fromiter(itertools.chain(*x), x.dtype)
            if x.dtype == object else sum(x))
        recs = recs.to_dict(orient='list')

        # lidx = idx[leaves]
        # lidx = np.unique(unq[leaves])
        # lxs = list(lidx['X'])
        # lys = list(lidx['Y'])

        # leave = pd.DataFrame(tdb.query(attrs=ma_list).multi_index[[lxs], [lys]])
        # leave = pd.DataFrame(tdb.query(attrs=ma_list).multi_index[[lxs], [lys]])

        # 3. Should rerun the metrics over them
        dx, dy, metrics = get_metrics([[],[],recs[att_list]], att_list, storage)
        ms = pd.DataFrame.from_dict(metrics)[[*ma_list, 'X', 'Y']]
        final = pd.concat((ms, leaves))

        # 4. output them to tifs
        for ma in ma_list:
            write_tif(x1, y1, final[ma].to_dict(orient='list'), ma, config)
