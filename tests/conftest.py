import pytest
import os
import dask
import pdal
from shutil import rmtree

from treetally import Bounds
from treetally.shatter import create_pipeline, shatter

@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope='session')
def filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def bounds(resolution, group_size, minx, maxx, miny, maxy, srs) -> Bounds:
    res = resolution
    gs = group_size

    yield Bounds(minx,miny,maxx,maxy,res,gs,srs)

@pytest.fixture(scope='session')
def pipeline(filepath) -> pdal.Pipeline:
    yield create_pipeline(filepath)

@pytest.fixture(scope='session')
def resolution() -> int:
    yield 30

@pytest.fixture(scope='session')
def group_size() -> int:
    yield 4

@pytest.fixture(scope='function')
def test_point_count() -> int:
    yield 84100

@pytest.fixture(scope='function')
def minx() -> float:
    yield 0

@pytest.fixture(scope='function')
def miny() -> float:
    yield 0

@pytest.fixture(scope='function')
def maxx() -> float:
    yield 300

@pytest.fixture(scope='function')
def maxy() -> float:
    yield 300

@pytest.fixture(scope="function")
def srs():
    yield 5070
