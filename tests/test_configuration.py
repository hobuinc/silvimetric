import pytest
import json
import os
import dataclasses


from silvimetric import StorageConfig, Bounds

@pytest.fixture(scope='function')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="function")
def config(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy,
           crs) -> StorageConfig:

    b = Bounds(minx, miny, maxx, maxy)
    config = StorageConfig(tdb_filepath, b, resolution, crs = crs,
                           attrs = attrs)
    yield config


class Test_Configuration(object):

    def test_serialization(self, config: StorageConfig):

        j = str(config)
        c = StorageConfig.from_string(j)
        assert config == c

