import pytest
import tiledb
import numpy as np
import os


from silvimetric import Storage, Extents, Bounds, Configuration
from silvimetric.cli import initialize

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="class")
def storage(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy, crs) -> Storage:
    b = Bounds(minx, miny, maxx, maxy)
    config = Configuration(tdb_filepath, b, resolution, crs = crs, attrs = attrs)
    yield Storage.create(config)


class Test_Storage(object):

    def test_schema(self, storage: Storage, attrs: list[str], dims):
        with storage.open('r') as st:
            s:tiledb.ArraySchema = st.schema
            assert s.has_attr('count')
            assert s.attr('count').dtype == np.int32

            for a in attrs:
                assert s.has_attr(a)
                assert s.attr(a).dtype == dims[a]

    def test_local(self, storage: Storage, attrs: list[str], dims):
        with storage.open('r') as st:
            sc = st.schema
            assert sc.has_attr('count')
            assert sc.attr('count').dtype == np.int32

            for a in attrs:
                assert sc.has_attr(a)
                assert sc.attr(a).dtype == dims[a]

    def test_config(self, storage: Storage):
        """Check that instantiation metadata is properly written"""
        
        storage.saveConfig()
        config = storage.getConfig()
        assert config.resolution == storage.config.resolution
        assert config.bounds == storage.config.bounds
        assert config.crs == storage.config.crs

# class Test_Initialize(object):
#     @pytest.skip(reason="Not finishes")
#     def test_command(self, cli_runner):
#         res = cli_runner.invoke(initialize, [])