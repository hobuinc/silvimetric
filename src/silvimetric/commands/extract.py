import math
from pathlib import Path
from typing import Dict

from osgeo import gdal, osr
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd


from .. import Storage, Extents, ExtractConfig
from ..commands.shatter import get_metrics, agg_list, join

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

def write_tif(xsize: int, ysize: int, rminx: float, rmaxy: float,
        data:np.ndarray, name: str, config: ExtractConfig) -> None:
    """
    Write out a raster with GDAL

    :param xsize: Length of X plane.
    :param ysize: Length of Y plane.
    :param data: Data to write to raster.
    :param name: Name of raster to write.
    :param config: ExtractConfig.
    """
    osr.UseExceptions()
    path = Path(config.out_dir) / f'{name}.tif'
    crs = config.crs
    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.to_wkt())
    # transform = [x, res, 0, y, 0, res]
    b = config.bounds

    transform = [b.minx, config.resolution, 0,
                 b.maxy, 0, -1*config.resolution]

    driver = gdal.GetDriverByName("GTiff")
    gdal_type = np_to_gdal_types[np.dtype(data.dtype).str]
    tif = driver.Create(str(path), int(xsize), int(ysize), 1, gdal_type)
    tif.SetGeoTransform(transform)
    tif.SetProjection(srs.ExportToWkt())
    tif.GetRasterBand(1).WriteArray(data)
    tif.GetRasterBand(1).SetNoDataValue(-9999)
    tif.FlushCache()
    tif = None

MetricDict = Dict[str, np.ndarray]
def get_metrics(data_in: MetricDict, config: ExtractConfig) -> MetricDict:
    """
    Reruns a metric over this cell. Only called if there is overlapping data.

    :param data_in: Cell data to be rerun.
    :param config: ExtractConfig.
    :return: Combined dict of attribute and newly derived metric data.
    """

    #TODO should just use the metric calculation methods from shatter
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

def handle_overlaps(config: ExtractConfig, storage: Storage, indices: np.ndarray) -> pd.DataFrame:
    """
    Handle cells that have overlapping data. We have to re-run metrics over these
    cells as there's no other accurate way to determined metric values. If there
    are no overlaps, this will do nothing.

    :param config: ExtractConfig.
    :param storage: Database storage object.
    :param indices: Indices with overlap.
    :return: Dataframe of rerun data.
    """

    ma_list = storage.getDerivedNames()
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
        #TODO fix this shit
        # data['X'] = data['X'] - data['X'].min()
        # data['Y'] = data['Y'] - data['Y'].min()

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
            if redo.empty:
                return data
            redo['X'] = redo['X'] - minx
            redo['Y'] = redo['Y'] - miny

        parts = min(int(redo['X'].max() * redo['Y'].max()), 1000)
        r = dd.from_pandas(redo, npartitions=parts)
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

def extract(config: ExtractConfig) -> None:
    """
    Pull data from database for each desired metric and output them to rasters

    :param config: ExtractConfig.
    """

    dask.config.set({"dataframe.convert-string": False})

    storage = Storage.from_db(config.tdb_dir)
    ma_list = storage.getDerivedNames()
    config.log.debug(f'Extracting metrics {[m for m in ma_list]}')
    root_bounds=storage.config.root

    e = Extents(config.bounds, config.resolution, root=root_bounds)
    i = e.get_indices()
    minx = i['x'].min()
    maxx = i['x'].max()
    miny = i['y'].min()
    maxy = i['y'].max()
    xsize = maxx - minx + 1
    ysize = maxy - miny + 1

    # TODO make sure that the data isn't being manipulated by the redo section
    final = handle_overlaps(config, storage, i).sort_values(['Y', 'X'])
    rminx = e.bounds.minx + (final.X.min() * config.resolution)
    rmaxy = e.bounds.maxy + (final.Y.min() * config.resolution)
    # transform = [638190.0, config.resolution, 0,
    #              4402380.0, 0, -1*config.resolution]

    # output metric data to tifs
    for ma in ma_list:
        m_data = np.full(shape=(ysize,xsize), fill_value=np.nan, dtype=final[ma].dtype)
        a = final[['X','Y',ma]].to_numpy()
        for x,y,md in a[:]:
            m_data[int(y)][int(x)] = md

        write_tif(xsize, ysize, rminx, rmaxy, m_data, ma, config)