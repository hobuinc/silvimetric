import pytest
import os
import dask
import pdal

from treetally import Chunk, Bounds
from treetally.shatter import create_pipeline


@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="Threads")

@pytest.fixture(scope='session')
def test_pointcloud() -> str:
    path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "1.2-with-color.copc.laz"
    )
    assert os.path.exists(path)
    return os.path.abspath(path)

@pytest.fixture(scope='session')
def pipeline(test_pointcloud) -> pdal.Pipeline:
    return create_pipeline(test_pointcloud)

@pytest.fixture(scope='function')
def test_point_count():
    return 1065

@pytest.fixture(scope='function')
def chunk(bounds):
    minx = 635619.85
    maxx = 638982.55
    miny = 848899.7
    maxy = 853535.43
    return Chunk(minx, maxx, miny, maxy, bounds)

@pytest.fixture(scope='function')
def bounds():
    res = 100
    gs = 16
    srs = 2991
    minx = 635619.85
    maxx = 638982.55
    miny = 848899.7
    maxy = 853535.43

    return Bounds(minx,maxx,miny,maxy,res,gs,srs)