import pytest
import os
import dask
import pdal

from silvimetric.extents import Extents, Bounds
from silvimetric.shatter import create_pipeline

@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="threads")

@pytest.fixture(scope='session')
def filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def bounds(minx, maxx, miny, maxy) -> Bounds:
    yield Bounds(minx, miny, maxx, maxy)

@pytest.fixture(scope='class')
def extents(resolution, tile_size, bounds, crs) -> Extents:
    yield Extents(bounds,resolution,tile_size,crs)

@pytest.fixture(scope="session")
def attrs() -> list[str]:
    yield [ 'Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity' ]

@pytest.fixture(scope="session")
def dims(attrs):
    yield { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in attrs }

@pytest.fixture(scope='class')
def pipeline(filepath) -> pdal.Pipeline:
    yield create_pipeline(filepath)

@pytest.fixture(scope='class')
def resolution() -> int:
    yield 30

@pytest.fixture(scope='class')
def tile_size() -> int:
    yield 4

@pytest.fixture(scope='class')
def test_point_count() -> int:
    yield 84100

@pytest.fixture(scope='class')
def minx() -> float:
    yield 300

@pytest.fixture(scope='class')
def miny() -> float:
    yield 300

@pytest.fixture(scope='class')
def maxx() -> float:
    yield 600

@pytest.fixture(scope='class')
def maxy() -> float:
    yield 600

@pytest.fixture(scope="class")
def crs():
    yield "EPSG:5070"
