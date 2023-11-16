import pytest
import tiledb
import numpy as np
import os
import click


from silvistat.storage import Storage
from silvistat.bounds import Bounds
from silvistat.cli import initialize

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="class")
def storage(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy, srs) -> Storage:
    yield Storage.create(attrs, resolution, [minx, miny, maxx, maxy],
                         tdb_filepath, srs)

class Test_Storage(object):

    def test_schema(self, storage: Storage, attrs: list[str], dims):
        with storage.open('r') as st:
            s = st.schema
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

    def test_metadata(self, storage: Storage, resolution: float, bounds: Bounds):
        """Check that instantiation metadata is properly written"""
        metadata = storage.getMetadata()
        assert metadata['resolution'] == resolution
        assert metadata['bounds'] == (bounds.minx, bounds.miny, bounds.maxx,
                                      bounds.maxy)
        assert metadata['crs'] == bounds.srs

        storage.saveMetadata({'foo': 'bar'})
        assert storage.mode == 'w'
        metadata =  storage.getMetadata()
        assert storage.mode == 'r'
        assert metadata['foo'] == 'bar'




# class Test_Initialize(object):
#     @pytest.skip(reason="Not finishes")
#     def test_command(self, cli_runner):
#         res = cli_runner.invoke(initialize, [])