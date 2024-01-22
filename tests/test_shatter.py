import os
import pytest
import numpy as np
import dask
import json
import uuid
import platform


from silvimetric.commands.shatter import shatter
from silvimetric.resources import Storage, Extents, ShatterConfig, Log

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

    @pytest.mark.skipif('linux' in platform.platform().lower(),
                        reason='Tiledb array_fragments bug in linux.')
    def test_shatter(self, shatter_config, storage: Storage, maxy):
        shatter(shatter_config)
        with storage.open('r') as a:
            y = a[:,0]['Z'].shape[0]
            x = a[0,:]['Z'].shape[0]
            assert y == 10
            assert x == 10
            for xi in range(x):
                for yi in range(y):
                    a[xi, yi]['Z'].size == 1
                    assert bool(np.all(a[xi, yi]['Z'][0] == ((maxy/storage.config.resolution)-yi)))
            m = storage.get_history()

        shatter_config.name = uuid.uuid4()
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
                    assert bool(np.all(a[xi, yi]['Z'][1] == ((maxy/storage.config.resolution)-yi)))

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

    @pytest.mark.parametrize('sh_cfg', ['shatter_config', 'uneven_shatter_config'])
    def test_sub_bounds(self, sh_cfg, test_point_count, request):
        s = request.getfixturevalue(sh_cfg)
        storage = Storage.from_db(s.tdb_dir)
        e = Extents.from_storage(s.tdb_dir)

        pc = 0
        for b in e.split():
            log = Log(20)
            sc = ShatterConfig(tdb_dir = s.tdb_dir,
                               log = log,
                               filename = s.filename,
                               tile_size = s.tile_size,
                               attrs = s.attrs,
                               metrics = s.metrics,
                               debug = s.debug,
                               bounds = b.bounds,
                               date=s.date)
            pc = pc + shatter(sc)
        history = storage.get_history()['shatter']
        assert isinstance(history, list)
        history = [ json.loads(h) for h in history ]
        pcs = [ h['point_count'] for h in history ]
        assert sum(pcs) == test_point_count
        assert pc == test_point_count

    @pytest.mark.skipif(
        os.environ.get('AWS_SECRET_ACCESS_KEY') is None or
        os.environ.get('AWS_ACCESS_KEY_ID') is None,
        reason='Missing necessary AWS environment variables'
    )
    def test_remote_creation(self, s3_shatter_config, s3_storage):
        dask.config.set(scheduler="processes")
        resolution = s3_storage.config.resolution
        maxy = s3_storage.config.root.maxy
        shatter(s3_shatter_config)
        with s3_storage.open('r') as a:
            y = a[:,0]['Z'].shape[0]
            x = a[0,:]['Z'].shape[0]
            assert y == 10
            assert x == 10
            for xi in range(x):
                for yi in range(y):
                    a[xi, yi]['Z'].size == 1
                    assert bool(np.all(a[xi, yi]['Z'][0] == ((maxy/resolution)-yi)))
