import numpy as np

import dask
import dask.array as da
import dask.bag as db

from typing import Dict

from ..resources import Bounds, Extents, Storage, Metric, Data
from ..resources import ShatterConfig, StorageConfig

def get_data(bounds: Bounds, filename: str, storageconfig: StorageConfig) -> np.ndarray:
    """
    Retrieve point data from Data object

    Parameters
    ----------
    bounds : Bounds
    filename : str
    storageconfig : StorageConfig

    Returns
    -------
    np.ndarray
    """
    data = Data(filename, storageconfig, bounds = bounds)
    data.execute()
    return data.array

def cell_indices(xpoints: np.ndarray, ypoints: np.ndarray, x: int, y: int) -> da.Array:
    return da.logical_and(xpoints == x, ypoints == y)

def get_atts(points: np.ndarray, chunk: Extents, attrs: list[str]) -> list[np.ndarray]:
    """
    Separate point data into respective cells and filter by requested stats

    Parameters
    ----------
    points : np.ndarray
    chunk : Extents
    attrs : list[str]

    Returns
    -------
    list[np.ndarray]
    """
    xis = da.floor(points[['xi']]['xi'])
    yis = da.floor(points[['yi']]['yi'])

    att_view = points[:][attrs]
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in chunk.indices]
    return dask.persist(*l)

type MetricDataIn = tuple[np.ndarray, np.ndarray, Dict[str, np.ndarray]]
def arrange(data: np.ndarray, chunk: Extents, attrs: list[str]) -> MetricDataIn:
    """
    Arrange data into a format that TileDB accepts, which is a dict of
    numpy object arrays, formated so that they won't advertize multi-dimensional
    size.

    Parameters
    ----------
    data : np.ndarray
    chunk : Extents
    attrs : list[str]

    Returns
    -------
    MetricDataIn

    Raises
    ------
    Exception
        If data is missing a requested attribute, then throw.
    """
    dd = {}
    for att in attrs:
        try:
            dd[att] = np.fromiter([*[np.array(col[att], col[att].dtype) for col in data], None], dtype=object)[:-1]
        except Exception as e:
            raise Exception(f"Missing attribute {att}: {e}")
    counts = np.array([z.size for z in dd['Z']], np.int32)

    ## remove empty indices
    empties = np.where(counts == 0)[0]
    dd['count'] = counts
    dx = chunk.indices['x']
    dy = chunk.indices['y']
    if bool(empties.size):
        for att in dd:
            dd[att] = np.delete(dd[att], empties)
        dx = np.delete(dx, empties)
        dy = np.delete(dy, empties)
    return (dx, dy, dd)


def get_metrics(data_in: MetricDataIn, attrs: list[str], metrics: list[Metric],
                tdb_dir: str)->np.int64:
    """
    Perform metric analysis over cell-chunked data. Runs every metric requested
    over every cell/statistic combination that it applies to.

    Parameters
    ----------
    data_in : MetricDataIn
    attrs : list[str]
    metrics : list[Metric]
    tdb_dir : str

    Returns
    -------
    np.int64
        Point count of this Extent
    """

    storage = Storage.from_db(tdb_dir)
    ## data comes in as [dx, dy, { 'att': [data] }]
    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return 0

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    metric_data = {
        f'{m.entry_name(attr)}': dask.persist(*[m(cell_data) for cell_data in data[attr]])
        for attr in attrs for m in metrics
    }
    full_data = data | metric_data

    storage.write(dx,dy,full_data)
    pc = data['count'].sum()
    return pc

def run(leaves: list[Extents], config: ShatterConfig, s_config: StorageConfig) -> np.int64:
    """
    Coordinate order of events for the shatter process in groups of dask bags.

    Parameters
    ----------
    leaves : list[Extents]
    config : ShatterConfig
    s_config : StorageConfig

    Returns
    -------
    np.int64
        Pointcount of the run
    """
    attrs = [a.name for a in config.attrs]

    leaves = db.from_sequence(leaves)
    points: db.Bag = leaves.map(get_data, config.filename, s_config).persist()
    att_data: db.Bag = points.map(get_atts, leaves, attrs).persist()
    arranged: db.Bag = att_data.map(arrange, leaves, attrs).persist()
    metrics: db.Bag = arranged.map(get_metrics, attrs, config.metrics, config.tdb_dir)

    vals = metrics.persist()

    return sum(vals)


def shatter(config: ShatterConfig) -> int:
    """
    Coordinate usage of ShatterConfig. Create extents, filter them so we don't
    do empty tile work, then send that list of extents to the runner.

    Parameters
    ----------
    config : ShatterConfig

    Returns
    -------
    int
        Point count of the dataset
    """

    config.log.debug('Filtering out empty chunks...')

    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    extents = Extents.from_sub(config.tdb_dir, config.bounds, config.tile_size)

    data = Data(config.filename, storage.config, extents.bounds)
    leaves = extents.chunk(data, 1000)

    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    pc = run(leaves, config, storage.config)
    config.point_count = int(pc)

    config.log.debug('Saving shatter metadata')
    storage.saveMetadata('shatter', str(config))
    return config.point_count
