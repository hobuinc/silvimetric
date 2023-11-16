import pytest
import tiledb
import os
from shutil import rmtree

from silvimetric.shatter import shatter


@pytest.mark.skip(reason="TODO reintroduce after splitting up commands")
class TestShatter(object):

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
