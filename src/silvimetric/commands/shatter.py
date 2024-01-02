from typing import Callable, Dict, Generator

import numpy as np

import dask
import dask.array as da
import dask.bag as db

from ..resources import Extents, Storage, ShatterConfig, Data, Bounds

def get_data(bounds: Bounds, filename: str, storage: Storage):
    """
    Retrieve point data from Data object

    Parameters
    ----------
    bounds : Bounds
    filename : str
    storage : Storage

    Returns
    -------
    np.ndarray
    """
    data = Data(filename, storage.config, bounds = bounds)
    data.execute()
    return data.array

type CellIndices = Callable[[np.ndarray, np.ndarray, int, int], da.Array]

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
    cell_indices: CellIndices = lambda xs, ys, x, y: da.logical_and(xs == x, ys == y)

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



def get_metrics(data_in: MetricDataIn, attrs: list[str], storage: Storage) -> int:
    """
    Run metric methods over each cell's attribute data.

    Parameters
    ----------
    data_in : MetricDataIn
    attrs : list[str]
    storage : Storage

    Returns
    -------
    int
        Sum of work cell point counts
    """

    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return 0

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    metric_data = {
        f'{m.entry_name(attr)}': dask.persist(*[m(cell_data) for cell_data in data[attr]])
        for attr in attrs for m in storage.config.metrics
    }
    full_data = data | metric_data

    storage.write(dx,dy,full_data)
    pc = data['count'].sum()
    return pc

type Leaves = Generator[Extents]
def run(leaves: Leaves, config: ShatterConfig, storage: Storage) -> int:
    """
    Coordinate workflow

    Parameters
    ----------
    leaves : Generator[Extents]
    config : ShatterConfig
    storage : Storage

    Returns
    -------
    int
        Sum of all point counts
    """
    attrs = [a.name for a in config.attrs]

    leaves = db.from_sequence(leaves)
    points: db.Bag = leaves.map(get_data, config.filename, storage).persist()
    att_data: db.Bag = points.map(get_atts, leaves, attrs).persist()
    arranged: db.Bag = att_data.map(arrange, leaves, attrs).persist()
    metrics: db.Bag = arranged.map(get_metrics, attrs, storage)

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
    leaves = extents.chunk(data, 100)

    # Begin main operations
    config.log.debug('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    config.point_count = int(pc)

    config.log.debug('Saving shatter metadata')
    storage.saveMetadata('shatter', str(config))
    return config.point_count
