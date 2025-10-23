from pathlib import Path

from osgeo import gdal, osr
import dask
import numpy as np
import pandas as pd

from .. import Storage, Extents, ExtractConfig, Bounds

np_to_gdal_types = {
    np.dtype(np.byte).str: gdal.GDT_Byte,
    np.dtype(np.uint8).str: gdal.GDT_Byte,
    np.dtype(np.int8).str: gdal.GDT_Int8,
    np.dtype(np.uint16).str: gdal.GDT_UInt16,
    np.dtype(np.int16).str: gdal.GDT_Int16,
    np.dtype(np.uint32).str: gdal.GDT_UInt32,
    np.dtype(np.int32).str: gdal.GDT_Int32,
    np.dtype(np.uint64).str: gdal.GDT_UInt64,
    np.dtype(np.int64).str: gdal.GDT_Int64,
    np.dtype(np.float32).str: gdal.GDT_Float32,
    np.dtype(np.float64).str: gdal.GDT_Float64,
}


def write_tif(
    bounds: Bounds,
    data: np.ndarray,
    nan_val: float | int,
    name: str,
    dtype: np.dtype,
    config: ExtractConfig,
) -> None:
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
    minx, _miny, _maxx, maxy = bounds.get()
    ysize, xsize = data.shape

    transform = [
        minx,
        config.resolution,
        0,
        maxy,
        0,
        -1 * config.resolution,
    ]

    driver = gdal.GetDriverByName('GTiff')
    gdal_type = np_to_gdal_types[dtype.str]
    tif = driver.Create(
        str(path),
        int(xsize),
        int(ysize),
        1,
        gdal_type,
    )
    tif.SetGeoTransform(transform)
    tif.SetProjection(srs.ExportToWkt())
    tif.GetRasterBand(1).SetNoDataValue(nan_val)
    tif.GetRasterBand(1).WriteArray(data)
    tif.FlushCache()
    tif = None


def get_data(
    config: ExtractConfig, storage: Storage, extents: Extents
) -> pd.DataFrame:
    """
    Handle cells that have overlapping data. We have to re-run metrics over
    these cells as there's no other accurate way to determined metric values.
    If there are no overlaps, this will do nothing.

    :param config: ExtractConfig.
    :param storage: Database storage object.
    :param indices: Indices with overlap.
    :return: Dataframe of rerun data.
    """

    ma_list = storage.get_derived_names(config.metrics, config.attrs)

    with storage.open('r', timestamp=config.timestamp) as tdb:
        xdim = tdb.schema.domain.dim('X').domain
        ydim = tdb.schema.domain.dim('Y').domain
        minx = max(extents.x1, xdim[0])
        maxx = min(extents.x2, xdim[1])
        miny = max(extents.y1, ydim[0])
        maxy = min(extents.y2, ydim[1])

        # older versions of silvimetric supported multiple values, and
        # for backwards compatibility we will try to accept it still
        data = tdb.query(
            attrs=[*ma_list],
            order='F',
            coords=True).df[minx:maxx - 1, miny:maxy - 1]

        # find values that are not unique, means they have multiple entries
        # TODO phase this out at some point, storage is no longer created
        # with duplicate entries as an option
        data = data.set_index(['Y', 'X'])
        dedup_data = data[data.index.duplicated(keep='last')]
        clean_data = data[~data.index.duplicated(False)]
        return pd.concat([clean_data, dedup_data])


def extract(config: ExtractConfig) -> None:
    """
    Pull data from database for each desired metric and output them to rasters

    :param config: ExtractConfig.
    """

    dask.config.set({'dataframe.convert-string': False})

    storage = Storage.from_db(config.tdb_dir)
    schema = storage.open('r').schema
    ma_list = storage.get_derived_names(config.metrics, config.attrs)
    config.log.debug(f'Extracting metrics {[m for m in ma_list]}')
    root_bounds = storage.config.root

    e = Extents(
        config.bounds,
        config.resolution,
        storage.config.alignment,
        root=root_bounds,
    )
    cell_size = 0
    for a in config.attrs:
        for m in config.metrics:
            if a in m.attributes:
                cell_size = cell_size + np.dtype(m.dtype).itemsize

    final = get_data(config, storage, e)
    futures = []
    for ma in ma_list:
        dtype = schema.attr(ma).dtype
        nan_val = -9999 if dtype.kind in ['i', 'f'] else 0
        if dtype.kind == 'u':
            nan_val = 0
        elif dtype.kind in ['i', 'f']:
            nan_val = -9999
        else:
            nan_val = 0
        unstacked = final[ma].unstack()
        unstacked = unstacked.fillna(nan_val)
        m_data = unstacked.to_numpy()

        futures.append(
            dask.delayed(write_tif)(e.bounds, m_data, nan_val, ma, dtype, config)
        )

    dask.compute(*futures)
