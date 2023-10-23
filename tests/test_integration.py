import pytest
import tiledb
import os
from shutil import rmtree

from treetally.shatter import shatter


class TestIntegration(object):

    @pytest.fixture(scope="class")
    def atts(self) -> list[str]:
        yield ['Z', 'Red', 'Green', 'Blue']

    @pytest.fixture(scope="class", autouse=True)
    def tdb_dir(self, filepath, group_size, resolution, atts) -> str:
        dir_name = os.path.join(os.path.dirname(__file__), "tdb_test_data")
        shatter(filepath, dir_name, group_size, resolution, debug=True,
                atts=atts)
        yield dir_name
        try:
            rmtree(dir_name)
        except:
            print('Tdb directory does not exist.')

    def test_pointcount(self, tdb_dir, test_point_count):
        from itertools import chain
        with tiledb.SparseArray(tdb_dir, mode='r') as tdb:
            #sum(len(sub_array) for sub_array in array)
            count = len(list(chain(tdb[:]['Z'])))
            assert(count == test_point_count)

    def test_attributes(self, tdb_dir, atts):
        schema = tiledb.ArraySchema.load(tdb_dir)
        attr_num = schema.nattr
        atts = [ *atts, "count" ]
        for i in range(attr_num):
            a = schema.attr(i)
            assert(a.name in atts)

        for i in atts:
            schema.attr(i)


