import pytest
import os
import numpy as np

from silvimetric.shatter import shatter, ShatterConfiguration
from silvimetric.storage import Storage, Configuration
from silvimetric.extract import extract, ExtractConfiguration

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs):
    yield Configuration(tdb_filepath, bounds, resolution, crs, attrs, 'test_version', name='test_db')

@pytest.fixture(scope='class')
def storage(storage_config) -> Storage:
    yield Storage.create(storage_config)

@pytest.fixture(scope='class')
def shatter_config(tdb_filepath, filepath, tile_size):
    yield ShatterConfiguration(tdb_filepath, filepath, tile_size, debug=True)

@pytest.fixture(scope='class')
def shatter_result(shatter_config):
    yield

class Test_Extract(object):

    def test_extract(self):
        pass