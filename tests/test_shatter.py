import pytest
import os
import numpy as np

from silvimetric.shatter import shatter, ShatterConfiguration
from silvimetric.storage import Storage, Configuration

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs):
    yield Configuration(tdb_filepath, bounds, resolution, crs, attrs, 'test_version', name='test_db')

@pytest.fixture(scope='class')
def shatter_config(tdb_filepath, filepath):
    yield ShatterConfiguration(tdb_filepath, filepath, 16, debug=True)

@pytest.fixture(scope="class")
def storage(storage_config) -> Storage:
    yield Storage.create(storage_config)

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
