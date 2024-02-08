import numpy as np
import signal
import datetime
import pandas as pd
from IPython.core.debugger import Pdb

from line_profiler import profile

import dask
from dask.distributed import as_completed, futures_of, CancelledError
import dask.array as da
import dask.bag as db

from tiledb import SparseArray

from ..resources import Extents, Storage, ShatterConfig, Data

def get_data(extents: Extents, filename: str, storage: Storage):
    data = Data(filename, storage.config, bounds = extents.bounds)
    p = data.pipeline
    data.execute()
    return p.get_dataframe(0)

@profile
def arrange(points: pd.DataFrame, leaf, attrs: list[str]):
    if points is None:
        return None
    if points.size == 0:
        return None

    points = points.loc[points.Y > leaf.bounds.miny]
    points = points.loc[points.X < leaf.bounds.maxx, [*attrs, 'xi', 'yi']]

    points.loc[:, 'xi'] = da.floor(points.xi)
    points.loc[:, 'yi'] = da.floor(points.yi)
    points = points.assign(count=lambda x: points.Z.count())
    grouped = points.groupby(['xi','yi']).agg(list)
    grouped = grouped.assign(count=lambda x: [len(z) for z in grouped.Z])

    return grouped

@profile
def get_metrics(data_in, attrs: list[str], storage: Storage):
    if data_in is None:
        return None

    import pickle
    f = open('./before', 'wb')
    pickle.dump(data_in, f)
    f.close()

    dfs = []
    for m in storage.config.metrics:
        data_in = data_in.assign(**{f'{m.entry_name(attr)}': lambda val: [m._method(v) for v in data_in[attr]] for attr in attrs})


    f = open('./after', 'wb')
    pickle.dump(data_in, f)
    f.close()
    return data_in

@profile
def write(data_in, tdb):
    if data_in is None:
        return 0

    idf = data_in.index.to_frame()
    dx = idf['xi'].to_list()
    dy = idf['yi'].to_list()

    # TODO get this working at some point. Look at pandas extensions
    # data_in = data_in.rename(columns={'xi':'X','yi':'Y'})

    # tiledb.from_pandas(uri='autzen_db', dataframe=data_in, mode='append',
    #         column_types=dict(data_in.dtypes),
    #         varlen_types=[np.dtype('O')])
    dt = lambda a: tdb.schema.attr(a).dtype
    dd = { d: np.fromiter([*[np.array(nd, dt(d)) for nd in data_in[d]], None], object)[:-1] for d in data_in }

    tdb[dx,dy] = dd
    return data_in['count'].sum().item()

def run(leaves: list[Extents], config: ShatterConfig, storage: Storage,
        tdb: SparseArray):

    start_time = config.start_time
    ## Handle a kill signal
    def kill_gracefully(signum, frame):
        client = dask.config.get('distributed.client')
        if client is not None:
            client.close()
        end_time = datetime.datetime.now().timestamp() * 1000
        config.end_time = end_time
        with storage.open(timestamp=(start_time, end_time)) as a:
            config.nonempty_domain = a.nonempty_domain()
            config.finished=False

            config.log.info('Saving config before quitting...')
            tdb.meta['shatter'] = str(config)
            config.log.info('Quitting.')

    signal.signal(signal.SIGINT, kill_gracefully)

    ## Handle dask bag transitions through work states
    attrs = [a.name for a in config.attrs]

    leaf_bag: db.Bag = db.from_sequence(leaves)
    points: db.Bag = leaf_bag.map(get_data, config.filename, storage)
    arranged: db.Bag = points.map(arrange, leaf_bag, attrs)
    metrics: db.Bag = arranged.map(get_metrics, attrs, storage)
    writes: db.Bag = metrics.map(write, tdb)

    ## If dask is distributed, use the futures feature
    if dask.config.get('scheduler') == 'distributed':
        pc_futures = futures_of(writes.persist())
        for batch in as_completed(pc_futures, with_results=True).batches():
            for future, pack in batch:
                if isinstance(pack, CancelledError):
                    continue
                for pc in pack:
                    config.point_count = config.point_count + pc

        end_time = datetime.datetime.now().timestamp() * 1000
        config.end_time = end_time
        config.finished = True
        a = storage.open(timestamp=(start_time, end_time))
        return config.point_count

    ## Handle non-distributed dask scenarios
    pc = sum(writes)
    end_time = datetime.datetime.now().timestamp() * 1000
    with storage.open(timestamp=(start_time, end_time)) as a:

        config.end_time = end_time
        config.finished=True
        config.point_count = pc
        config.nonempty_domain = a.nonempty_domain()
        tdb.meta['shatter'] = str(config)

    return pc


def shatter(config: ShatterConfig):
    # get start time in milliseconds
    config.start_time = datetime.datetime.now().timestamp() * 1000
    # set up tiledb
    storage = Storage.from_db(config.tdb_dir)
    data = Data(config.filename, storage.config, config.bounds)
    extents = Extents.from_sub(config.tdb_dir, data.bounds)


    with storage.open('w') as tdb:

        ## shatter should still output a config if it's interrupted
        # TODO add slices yet to be finished so we can pick up this process
        # in the future

        if config.bounds is None:
            config.bounds = extents.bounds

        config.log.debug('Grabbing leaf nodes...')
        if config.tile_size is not None:
            leaves = extents.get_leaf_children(config.tile_size)
        else:
            leaves = extents.chunk(data, 100)

        # Begin main operations
        config.log.debug('Fetching and arranging data...')
        pc = run(leaves, config, storage, tdb)
        config.point_count = int(pc)

        config.log.debug('Saving shatter metadata')
        tdb.meta['shatter'] = str(config)
        return config.point_count
