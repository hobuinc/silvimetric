import pytest
import os
import numpy as np
import dask

from silvimetric.shatter import shatter, ShatterConfiguration
from silvimetric.storage import Storage, Configuration
from silvimetric.metric import Metrics


@dask.delayed
def write(x,y,val, s:Storage, attrs, dims, metrics):
    m_list = [Metrics[m].att(a) for m in metrics for a in attrs]
    data = { att: np.array([np.array([val], dims[att]), None], object)[:-1]
                for att in attrs }

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

            shatter(shatter_config)
            a.reopen()
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
        assert storage.getMetadata('shatter') is not None
        assert storage.getMetadata('point_count') is not None

        assert storage.getMetadata('point_count') == test_point_count
