from pathlib import Path

from dask.diagnostics import ProgressBar
from distributed.client import _get_global_client as get_client
from typing_extensions import Union
from osgeo import gdal, osr
import dask
import numpy as np
import pandas as pd


from .. import Storage, Extents, ExtractConfig, Bounds, Graph

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
    minx, miny, maxx, maxy = bounds.get()
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
    gdal_type = np_to_gdal_types[np.dtype(data.dtype).str]
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


def get_metrics(
    data_in: pd.DataFrame, storage: Storage
) -> Union[None, pd.DataFrame]:
    """
    Reruns a metric over this cell. Only called if there is overlapping data.

    :param data_in: Dataframe to be rerun.
    :param storage: Base storage object.
    :return: Combined dict of attribute and newly derived metric data.
    """

    # TODO should just use the metric calculation methods from shatter
    if data_in is None:
        return None

    def expl(x):
        return x.explode()

    attrs = [a.name for a in storage.config.attrs if a.name not in ['X', 'Y']]

    # set index so we can apply to the whole dataset without needing to skip X
    # and Y then reset in the index because that's what metric.do expects
    data_in = data_in.set_index(['Y', 'X'])
    exploded = data_in.apply(expl)[attrs].reset_index()

    exploded.rename(columns={'X': 'xi', 'Y': 'yi'}, inplace=True)
    graph = Graph(storage.config.metrics)
    metric_data = graph.run(exploded)
    # rename index from xi,yi to X,Y
    metric_data.index = metric_data.index.rename(['Y', 'X'])

    return metric_data


def handle_overlaps(
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
    att_list = [a.name for a in config.attrs]

    minx = extents.x1
    maxx = extents.x2
    miny = extents.y1
    maxy = extents.y2

    att_meta = {}
    att_meta['X'] = np.int32
    att_meta['Y'] = np.int32
    att_meta['count'] = np.int32
    for a in config.attrs:
        att_meta[a.name] = a.dtype

    with storage.open('r', timestamp=config.timestamp) as tdb:
        storage.config.log.info('Looking for overlaps...')
        data = tdb.query(
            attrs=[*ma_list],
            order='F',
            coords=True,
        ).df[minx:maxx, miny:maxy]

        # find values that are not unique, means they have multiple entries
        data = data.set_index(['Y', 'X'])
        redo_indices = data.index[data.index.duplicated(keep='first')]
        if redo_indices.empty:
            storage.config.log.info('No overlapping data. Continuing...')
            return data

        redo_data = (
            tdb.query(
                attrs=[*att_list],
                order='F',
                coords=True,
                # use_arrow=False,
            )
            .df[:, :]
            .set_index(['Y', 'X'])
        )

        # data with overlaps
        redo_data = redo_data.loc[redo_indices]

        # data that has no overlaps
        clean_data = data.loc[data.index[~data.index.duplicated(False)]]

        storage.config.log.warning(
            'Overlapping data detected. Rerunning metrics over these cells...'
        )
        new_metrics = get_metrics(redo_data.reset_index(), storage)
    return pd.concat([clean_data, new_metrics])


def extract(config: ExtractConfig) -> None:
    """
    Pull data from database for each desired metric and output them to rasters

    :param config: ExtractConfig.
    """
    import dask.dataframe as ddf
    import dask.array as da
    dask.config.set({'dataframe.convert-string': False})

    storage = Storage.from_db(config.tdb_dir)
    ma_list = storage.get_derived_names(config.metrics, config.attrs)
    config.log.debug(f'Extracting metrics {[m for m in ma_list]}')
    root_bounds = storage.config.root

    e = Extents(
        config.bounds,
        config.resolution,
        storage.config.alignment,
        root=root_bounds,
    )

    # figure out if there are any overlaps and handle them
    # final = handle_overlaps(config, storage, e)
    final = da.from_tiledb(config.tdb_dir)[ma_list]


    xis = final.index.get_level_values(1).astype(np.int64)
    yis = final.index.get_level_values(0).astype(np.int64)
    new_idx = pd.MultiIndex.from_product(
        (range(yis.min(), yis.max() + 1), range(xis.min(), xis.max() + 1))
    ).rename(['Y', 'X'])
    final = final.reindex(new_idx)

    xs = root_bounds.minx + xis * config.resolution
    ys = root_bounds.maxy - yis * config.resolution
    final_bounds = Bounds(xs.min(), ys.min(), xs.max(), ys.max())

    # output metric data to tifs
    config.log.info(f'Writing rasters to {config.out_dir}')
    futures = []
    for ma in ma_list:
        # TODO should output in sections so we don't run into memory problems
        dtype = final[ma].dtype
        if dtype.kind == 'u':
            nan_val = 0
        elif dtype.kind in ['i', 'f']:
            nan_val = -9999
        else:
            nan_val = 0
        unstacked = final[ma].unstack()
        m_data = unstacked.to_numpy()

        futures.append(
            dask.delayed(write_tif)(final_bounds, m_data, nan_val, ma, config)
        )

    dc = get_client()
    if dc is not None:
        dask.compute(*futures)
    else:
        with ProgressBar():
            dask.compute(*futures)
