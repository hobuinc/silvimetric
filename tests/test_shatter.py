import pytest
import os

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

    def test_shatter(self, shatter_config, storage: Storage):
        shatter(shatter_config)
        with storage.open('r') as a:
            assert a[0,0]['Z'][0][0] == 20.0
