import pdal
import numpy as np
from line_profiler import profile

import dask
import dask.array as da
import dask.bag as db
from dask.distributed import performance_report, Client

from ..resources import Bounds, Extents, Storage, Metric, ShatterConfig, ApplicationConfig

@dask.delayed
@profile
def get_data(filename, chunk):
    pipeline = create_pipeline(filename, chunk)
    try:
        pipeline.execute()
    except Exception as e:
        print(pipeline.pipeline, e)

    return pipeline.arrays[0]

def cell_indices(xpoints, ypoints, x, y):
    return da.logical_and(xpoints == x, ypoints == y)

@dask.delayed
@profile
def get_atts(points: da.Array, chunk: Extents, attrs: list[str]):
    xis = da.floor(points[['xi']]['xi'])
    yis = da.floor(points[['yi']]['yi'])

    att_view = points[:][attrs]
    l = [att_view[cell_indices(xis, yis, x, y)] for x,y in chunk.indices]
    return dask.compute(*l)

@dask.delayed
@profile
def arrange(chunk, data, attrs):
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
    return [dx, dy, dd]


@dask.delayed
@profile
def get_metrics(data_in, attrs: list[str], metrics: list[Metric],
                storage: Storage):
    ## data comes in as [dx, dy, { 'att': [data] }]
    dx, dy, data = data_in

    # make sure it's not empty. No empty writes
    if not np.any(data['count']):
        return 0

    # doing dask compute inside the dict array because it was too fine-grained
    # when it was outside
    metric_data = {
            f'{m.entry_name(attr)}': dask.compute(*[m(cell_data) for cell_data in data[attr]])
            for attr in attrs for m in metrics
        }
    full_data = data | metric_data
    storage.write(dx,dy,full_data)
    pc = data['count'].sum()
    return pc


def create_pipeline(filename, chunk):
    reader = pdal.Reader(filename, tag='reader')
    reader._options['threads'] = 2
    reader._options['bounds'] = str(chunk)
    class_zero = pdal.Filter.assign(value="Classification = 0")
    rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
    nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
    ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")
    assign_x = pdal.Filter.assign(
        value=f"xi = (X - {chunk.root.minx}) / {chunk.resolution}")
    assign_y = pdal.Filter.assign(
        value=f"yi = ({chunk.root.maxy} - Y) / {chunk.resolution}")
    # smrf = pdal.Filter.smrf()
    # hag = pdal.Filter.hag_nn()
    # return reader | crop | class_zero | rn | nor #| smrf | hag
    return reader | class_zero | rn | nor | ferry | assign_x | assign_y #| smrf | hag

def one(leaf: Extents, config: ShatterConfig, storage: Storage):
    attrs = [a.name for a in config.attrs]

    points = get_data(config.filename, leaf)
    att_data = get_atts(points, leaf, attrs)
    arranged = arrange(leaf, att_data, attrs)
    m = get_metrics(arranged, attrs, config.metrics, storage)
    return dask.compute(m)[0]

def run(leaves, config: ShatterConfig, storage: Storage, client: Client=None):
    from contextlib import nullcontext
    l = []

    with (performance_report() if client is not None else nullcontext()):
        leaves = db.from_sequence(leaves)
        l = db.map(one, leaves, config, storage)
        vals = l.compute()

    return sum(vals)

def shatter(config: ShatterConfig, client: Client=None):

    config.app.log.debug('Filtering out empty chunks...')
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    extents = Extents.from_sub(storage, config.bounds, config.tile_size)

    leaves = extents.chunk(config.filename, 1000)

    # Begin main operations
    config.app.log.debug('Fetching and arranging data...')
    pc = run(leaves, config, storage)
    config.point_count = int(pc)

    config.app.log.debug('Saving shatter metadata')
    storage.saveMetadata('shatter', str(config))
    return config.point_count
