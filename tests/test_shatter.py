import pytest
import os

from silvimetric.shatter import shatter
from silvimetric.storage import Storage

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="class")
def storage(tdb_filepath, resolution, attrs, minx, maxx, miny, maxy, srs) -> Storage:
    yield Storage.create(attrs, resolution, [minx, miny, maxx, maxy],
                         tdb_filepath, srs)

class Test_Shatter(object):

    def test_shatter(self, tdb_filepath, filepath: str, storage: Storage):
        shatter(filepath, tdb_filepath, 16, debug=True)
        with storage.open('r') as a:
            data = a.query(attrs=['Z'], order='C', coords=True)[:]
            z = data['Z']
            print(z)