import pytest
import numpy as np
import dask
import json
import uuid
import copy

from silvimetric import shatter
from silvimetric import Storage, Extents, ShatterConfig


@dask.delayed
def write(x,y,val, s:Storage, attrs, dims, metrics):
    m_list = [m.entry_name(a.name) for m in metrics for a in attrs]
    data = { a.name: np.array([np.array([val], dims[a.name]), None], object)[:-1]
                for a in attrs }

    for m in m_list:
        data[m] = [val]

    data['count'] = [val]
    with s.open('w') as w:
        w[x,y] = data

class Test_Shatter(object):

    def test_shatter(self, shatter_config, storage: Storage, resolution, maxy):
        shatter(shatter_config)
        with storage.open('r') as a:
            y = a[:,0]['Z'].shape[0]
            x = a[0,:]['Z'].shape[0]
            assert y == 10
            assert x == 10
            for xi in range(x):
                for yi in range(y):
                    a[xi, yi]['Z'].size == 1
                    assert bool(np.all(a[xi, yi]['Z'][0] == ((maxy/resolution)-yi)))
            m = storage.get_history()

        shatter_config.name = uuid.uuid1()
        shatter(shatter_config)
        with storage.open('r') as a:
            # querying flattens to 20, there will 10 pairs of values
            assert a[:,0]['Z'].shape[0] == 20
            assert a[0,:]['Z'].shape[0] == 20
            # now test that the second set is the same as the first set
            # and test that this value is still the same as the original
            # which was set at ((maxy/resolution)-yindex)
            for xi in range(x):
                for yi in range(y):
                    a[xi, yi]['Z'].size == 2
                    assert bool(np.all(a[xi, yi]['Z'][1] == a[xi,yi]['Z'][0]))
                    assert bool(np.all(a[xi, yi]['Z'][1] == ((maxy/resolution)-yi)))

            m2 = storage.get_history()
            assert len(m2['shatter']) == 2

    def test_parallel(self, storage, attrs, dims, threaded_dask, metrics):
        # test that writing in parallel doesn't affect ordering of values
        # constrained by NumberOfReturns being uint8

        count = 255
        tl = [write(0,0,val,storage,attrs,dims, metrics) for val in range(count)]

        dask.compute(tl)

        with storage.open('r') as r:
            d = r[0,0]
            for idx in range(count):
                assert bool(np.all(d['Z'][idx] == d['Intensity'][idx]))
                assert bool(np.all(d['Intensity'][idx] == d['NumberOfReturns'][idx]))
                assert bool(np.all(d['NumberOfReturns'][idx] == d['ReturnNumber'][idx]))

    def test_config(self, shatter_config, storage, test_point_count):
        shatter(shatter_config)
        try:
            meta = storage.getMetadata('shatter')
        except BaseException as e:
            pytest.fail("Failed to retrieve 'shatter' metadata key." + e.args)
        meta_j = json.loads(meta)
        pc = meta_j['point_count']
        assert pc == test_point_count

    def test_sub_bounds(self, shatter_config, storage, test_point_count):
        s = shatter_config
        e = Extents.from_storage(storage, s.tile_size)
        pc = 0
        for b in e.split():
            sc = ShatterConfig(s.tdb_dir, s.filename, s.tile_size, s.attrs,
                               s.metrics, s.debug, bounds=b.bounds)
            pc = pc + shatter(sc)
        history = storage.get_history()['shatter']
        assert isinstance(history, list)
        history = [ json.loads(h) for h in history ]
        pcs = [ h['point_count'] for h in history ]
        assert sum(pcs) == test_point_count
        assert pc == test_point_count
