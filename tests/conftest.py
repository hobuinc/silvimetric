import pytest
import os
import dask
import pdal
from shutil import rmtree

from treetally import Chunk, Bounds
from treetally.shatter import create_pipeline, shatter

# @pytest.fixture(scope='session', autouse=True)
# def term_handler():
#     orig = signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGINT))
#     yield
#     signal.signal(signal.SIGTERM, orig)

@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope='session')
def filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "1.2-with-color.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

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
def chunk(bounds):
    minx = 635619.85
    maxx = 638982.55
    miny = 848899.7
    maxy = 853535.43
    yield Chunk(minx, maxx, miny, maxy, bounds)

@pytest.fixture(scope='function')
def bounds(resolution, group_size):
    res = resolution
    gs = group_size
    srs = 2991
    minx = 635619.85
    maxx = 638982.55
    miny = 848899.7
    maxy = 853535.43

    yield Bounds(minx,maxx,miny,maxy,res,gs,srs)