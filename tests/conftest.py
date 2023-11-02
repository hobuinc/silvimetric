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
            "1.2-with-color.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def bounds(resolution, group_size, minx, maxx, miny, maxy, srs):
    res = resolution
    gs = group_size

    yield Bounds(minx,miny,maxx,maxy,res,gs,srs)

@pytest.fixture(scope='session')
def pipeline(filepath) -> pdal.Pipeline:
    yield create_pipeline(filepath)

@pytest.fixture(scope='session')
def resolution() -> int:
    yield 100

@pytest.fixture(scope='session')
def group_size() -> int:
    yield 16

@pytest.fixture(scope='function')
def test_point_count():
    yield 1065

@pytest.fixture(scope='function')
def minx():
    yield 635619.85

@pytest.fixture(scope='function')
def miny():
    yield 848899.7

@pytest.fixture(scope='function')
def maxx():
    yield 638982.55

@pytest.fixture(scope='function')
def maxy():
    yield 853535.43

@pytest.fixture(scope="function")
def srs():
    yield 2991
