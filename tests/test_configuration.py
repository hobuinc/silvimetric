import pytest
import tiledb
import json
import numpy as np
import os


from silvimetric import Configuration, Bounds

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="class")
def config(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy, crs) -> Configuration:
    b = Bounds(minx, miny, maxx, maxy)
    config = Configuration(tdb_filepath, b, resolution, crs = crs, attrs = attrs)
    yield config


class Test_Configuration(object):

    def test_serialization(self, config: Configuration):
        assert config.resolution == float('30.0')

        j = config.to_json()
        j = json.dumps(j)

        c = Configuration.from_json(j)
        assert c.resolution == float('30.0')
