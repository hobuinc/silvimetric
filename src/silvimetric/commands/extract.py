from osgeo import gdal, osr
import numpy as np
from pathlib import Path
import pandas as pd
import dask
import dask.dataframe as dd
import dask.bag as db

from ..resources import Storage, ExtractConfig, Metric, Attribute
from ..resources import Extents

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

def get_metrics(data_in, config: ExtractConfig):
    if data_in is None:
        return None

    # make sure it's not empty. No empty writes
    if not np.any(data_in['count']):
        return None

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    #TODO move this operation to a dask bag and convert to dataframe and then
    # to a dict for better efficiency?
    metric_data = {
        f'{m.entry_name(attr.name)}': [m(cell_data) for cell_data in data_in[attr.name]]
        for attr in config.attrs for m in config.metrics
    }
    # md = dask.compute(metric_data)[0]
    data_out = data_in | metric_data
    return data_out

def handle_overlaps(config: ExtractConfig, storage: Storage, indices: np.ndarray):
    ma_list = [ m.entry_name(a.name) for m in config.metrics for a in
        config.attrs ]
    att_list = [ a.name for a in config.attrs ] + [ 'count' ]
    out_list = [ *ma_list, 'X', 'Y' ]

    minx = indices['x'].min()
    maxx = indices['x'].max()
    miny = indices['y'].min()
    maxy = indices['y'].max()

    att_meta = {}
    att_meta['X'] = np.int32
    att_meta['Y'] = np.int32
    att_meta['count'] = np.int32
    for a in config.attrs:
        att_meta[a.name] =  a.dtype

    with storage.open("r") as tdb:
        data = tdb.query(attrs=ma_list, order='F', coords=True).df[minx:maxx,
                miny:maxy]
        data['X'] = data['X'] - minx
        data['Y'] = data['Y'] - miny

        # 1. should find values that are not unique, meaning they have multiple
        # entries
        recarray = data.to_records()
        xys = recarray[['X', 'Y']]
        unq, idx, counts = np.unique(xys, return_index=True, return_counts=True)
        redos = np.where(counts >= 2)

        if not np.any(redos):
            return data

        leaves = data.loc[idx[np.where(counts == 1)]]

        # 2. then should combine the values of those attribute/cell combos
        ridx = np.unique(unq[redos])
        rxs = list(ridx['X'])
        rys = list(ridx['Y'])
        if np.any(rxs):
            redo = pd.DataFrame(tdb.query(attrs=att_list).multi_index[[rxs], [rys]])

        parts = min(int(redo['X'].max() * redo['Y'].max()), 1000)
        r = dd.from_pandas(redo, npartitions=parts, name='MetricDataFrame')
        def squash(x):
            def s2(vals):
                if vals.dtype == object:
                    return np.array([*vals], vals.dtype).reshape(-1)
                elif vals.name == 'X' or vals.name == 'Y':
                    return vals.values[0]
                else:
                    return sum(vals)
            val = x.aggregate(s2)
            p = pd.DataFrame(val).T
            return p

        recs = r.groupby(['X','Y']).apply(squash, meta=att_meta)
        rs = recs.compute().to_dict(orient='list')

        # 3. Should rerun the metrics over them
        metrics = get_metrics(rs, config)
        ms = pd.DataFrame.from_dict(metrics)[out_list]
        return pd.concat((ms, leaves[out_list]))

def extract(config: ExtractConfig):

    dask.config.set({"dataframe.convert-string": False})

    ma_list = [ m.entry_name(a.name) for m in config.metrics for a in
        config.attrs ]


    storage = Storage.from_db(config.tdb_dir)
    root_bounds=storage.config.root

    e = Extents(config.bounds, config.resolution, root=root_bounds)
    i = e.get_indices()
    minx = i['x'].min()
    maxx = i['x'].max()
    miny = i['y'].min()
    maxy = i['y'].max()
    x1 = maxx - minx + 1
    y1 = maxy - miny + 1

    final = handle_overlaps(config, storage, i)

    # output metric data to tifs
    xs = final['X'].max() + 1
    ys = final['Y'].max() + 1
    for ma in ma_list:
        raster_data = np.full((ys, xs), np.nan)
        raster_idx = final['Y']*ys + final['X']
        np.put(raster_data, raster_idx, final[ma])
        write_tif(x1, y1, raster_data, ma, config)