import pytest
import tiledb
import os
from shutil import rmtree

from treetally.shatter import shatter


class TestIntegration(object):

    @pytest.fixture(scope="class")
    def tdb_dir(self) -> str:
        dir_name = os.path.join(os.path.dirname(__file__), "tdb_test_data")
        yield dir_name

    @pytest.fixture(scope="class")
    def atts(self) -> list[str]:
        yield ['Z', 'Red', 'Green', 'Blue']

    @pytest.fixture(scope="class", autouse=True)
    def tdb_data(self, filepath, tdb_dir, group_size, resolution, atts) -> None:
        shatter(filepath, tdb_dir, group_size, resolution, debug=True,
                atts=atts)
        yield
        try:
            rmtree(tdb_dir)
        except:
            print('Tdb directory does not exist.')

    def test_pointcount(self, tdb_dir):
        with tiledb.SparseArray(tdb_dir, mode='r') as tdb:
            a = tdb
        pass

    def test_attributes(self, resolution, group_size, filepath, tdb_dir):
        test_atts = ['Z', 'Red', 'Green', 'Blue']

        shatter(filepath, tdb_dir, group_size, resolution, True, atts=test_atts)
