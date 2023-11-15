import pytest
import tiledb
import numpy as np
import os
import click


from silvistat.storage import Storage
from silvistat.cli import initialize

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="class")
def storage(self, tdb_filepath, resolution, attrs, minx, maxx, miny, maxy) -> Storage:
    s = Storage()
    s.create(attrs, resolution, [minx, miny, maxx, maxy], tdb_filepath)
    yield s

class Test_Storage(object):

    def test_schema(self, storage: Storage, attrs: list[str], dims):
        s = storage.schema

        assert s.has_attr('count')
        assert s.attr('count').dtype == np.int32

        for a in attrs:
            assert s.has_attr(a)
            assert s.attr(a).dtype == dims[a]


    def test_local(self, storage: Storage, attrs: list[str], dims):
        s = tiledb.ArraySchema.load(storage.path)

        assert s.has_attr('count')
        assert s.attr('count').dtype == np.int32

        for a in attrs:
            assert s.has_attr(a)
            assert s.attr(a).dtype == dims[a]

@pytest.skip(reason="Not finishes")
class Test_Initialize(object):
    def test_command(self, cli_runner):
        res = cli_runner.invoke(initialize, [])