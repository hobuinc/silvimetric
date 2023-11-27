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
def tif_filepath(tmp_path_factory) -> str:
    yield os.path.join(os.path.dirname(__file__), "data",
            "tif_out")

    # path = tmp_path_factory.mktemp("test_tifs")
    # yield os.path.abspath(path)

@pytest.fixture(scope='class')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs):
    yield Configuration(tdb_filepath, bounds, resolution, crs, attrs, 'test_version', name='test_db')

@pytest.fixture(scope='class', autouse=True)
def storage(storage_config) -> Storage:
    yield Storage.create(storage_config)

@pytest.fixture(scope='class')
def shatter_config(tdb_filepath, filepath, tile_size):
    config = ShatterConfiguration(tdb_filepath, filepath, tile_size, debug=True)
    shatter(config)
    yield config

@pytest.fixture(scope='class')
def extract_config(tdb_filepath, tif_filepath):
    yield ExtractConfiguration(tdb_filepath, tif_filepath)

class Test_Extract(object):

    def test_config(self, extract_config, tdb_filepath, tif_filepath, attrs):
        assert all([a in [*attrs, 'count'] for a in extract_config.attrs])
        assert all([a in extract_config.attrs for a in attrs])
        assert extract_config.tdb_dir == tdb_filepath
        assert extract_config.out_dir == tif_filepath

    def test_extract(self, extract_config):
        extract(extract_config)