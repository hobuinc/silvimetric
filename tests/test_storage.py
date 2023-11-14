import pytest
import tiledb
import numpy as np

from silvistat.storage import Storage

class Test_Storage(object):
    @pytest.fixture(scope="class")
    def storage(self, tdb_filepath, filepath, resolution, group_size, attrs) -> Storage:
        s = Storage(tdb_filepath, filepath, resolution, group_size)
        s.create(attrs)
        yield s

    def test_schema(self, storage: Storage, attrs: list[str], dims):
        s = storage.schema

        assert s.has_attr('count')
        assert s.attr('count').dtype == np.int32

        for a in attrs:
            assert s.has_attr(a)
            assert s.attr(a).dtype == dims[a]


    def test_local(self, storage: Storage, attrs: list[str], dims):
        s = tiledb.ArraySchema.load(storage.dirname)

        assert s.has_attr('count')
        assert s.attr('count').dtype == np.int32

        for a in attrs:
            assert s.has_attr(a)
            assert s.attr(a).dtype == dims[a]
