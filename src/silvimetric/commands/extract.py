from osgeo import gdal, osr
import numpy as np
import numpy.lib.recfunctions as rfn
from pathlib import Path
import awkward as ak
import pandas as pd

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
        data = tdb.query(attrs=ma_list, order='F', coords=True).df[minx:maxx,
                miny:maxy]
        data['X'] = data['X'] - minx
        data['Y'] = data['Y'] - miny

        # 1. should find values that are not unique, meaning they have multiple
            # entries
        recarray = data.to_records(index=False)
        xys = recarray[['X', 'Y']]
        unq, inv, counts = np.unique(xys, return_inverse=True,
                return_counts=True)
        redos = np.where(counts >= 2)
        leave = np.where(counts == 1)

        # 2. then should combine the values of those attribute/cell combos
        # TODO update extract config to use attributes and metrics, not strings
        att_list = [a.name for a in config.attrs]
        xs = np.unique(unq[redos]['X'])
        ys = np.unique(unq[redos]['Y'])
        redo_cells = pd.DataFrame(tdb.multi_index[[list(xs)], [list(ys)]]).to_records()
        result = np.recarray(redos[0].shape, dtype=redo_cells.dtype)

        for idx, val in enumerate(redos[0]):
            vals = redo_cells[np.where(redo_cells[['X', 'Y']] == unq[val])]
            result[idx] = rfn.merge_arrays(vals)

        metrics = get_metrics([[],[],redo_cells], att_list, storage)


        # 3. Should rerun the metrics over them
            # - Add method to metric to get the attribute name from entry name

        for ma in ma_list:

            # m_data = ak.Array(shape=(y1,x1), fill_value=np.nan, dtype=data[ma].dtype)

            # create numpy array of x and y, then get values from tiledb, then
            # broadcast with awkward array?

            # a = np.array(data[['X','Y', ma]].to_numpy().reshape(-1), dtype=[('X', np.int32), ('Y', np.int32), (ma, data[ma].dtype)])
            # l = a.shape[0]
            # a2 = a.reshape(-1)
            # np.unique(a, return_counts=True)
            axis = data[['X','Y']].to_numpy()
            li = [np.array()]
            awk = ak.from_iter([ak.from_iter(d[x,y][ma], ) for x,y in axis ])
            ak.where(ak.count(awk,1) > 1)



            # TODO figure out which cells have multiple values for metrics
            # and recompute those metrics with combined cell data
            a2 = data[ma].to_numpy().reshape(y1,x1,-1)
            asdf = np.where(len(a2) > 1)


            # np.unique(a, return_index=True)
            # for x,y,md in a[:]:
            #     if np.isnan(m_data[int(y)][int(x)]):
            #         m_data[int(y)][int(x)] = md
            #     else:
            #         m_data[int(y)][int(x)] = np.append((m_data[int(y)][int(x)], md))


            write_tif(x1, y1, a2, ma, config)
