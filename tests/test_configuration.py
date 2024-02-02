import pytest
import json
import os
import dataclasses


from silvimetric.resources import StorageConfig, Bounds, Log, Metric

@pytest.fixture(scope='function')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="function")
def config(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy,
           crs) -> StorageConfig:

    b = Bounds(minx, miny, maxx, maxy)
    log = Log(20)
    config = StorageConfig(tdb_dir = tdb_filepath,
                           log = log,
                           root = b,
                           resolution = resolution,
                           crs = crs,
                           attrs = attrs)
    yield config


class Test_Configuration(object):

    def test_serialization(self, config: StorageConfig):

        j = str(config)
        c = StorageConfig.from_string(j)
        mean = [ m for m in c.metrics if m.name == 'mean']
        assert len(mean) == 1

        assert int(mean[0]([2,2,2,2])) == 2
        assert config == c

