import pytest
import os
import dask
import pdal

from treetally import Chunk, Bounds
from treetally.shatter import create_pipeline

res = 100
gs = 16
srs = 2992
minx = 635579.19
maxx = 639003.73
miny = 848887.49
maxy = 853534.37

point_count = 61201
@pytest.fixture(scope='session')
def point_count():
    return point_count

@pytest.fixture(scope="session")
def autzen_classified() -> str:
    path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "autzen_test.copc.laz"
    )
    assert os.path.exists(path)
    return os.path.abspath(path)

@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="Threads")

@pytest.fixture(scope='session')
def pipeline(autzen_classified) -> pdal.Pipeline:
    return create_pipeline(autzen_classified)

@pytest.fixture(scope='session')
def chunk(bounds):
    return Chunk(minx, maxx, miny, maxy, bounds)

@pytest.fixture(scope='session', autouse=True)
def bounds():
    return Bounds(minx,maxx,miny,maxy,res,gs,srs)