import pytest
import json
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

        j = str(config)
        c = Configuration.from_string(j)
        assert c == config
