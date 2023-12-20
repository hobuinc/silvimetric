import pytest
import os
import dask
import pdal

from silvimetric.resources import Extents, Bounds, Metrics, Attribute, Storage, Log
from silvimetric.resources.config import ShatterConfig, StorageConfig, ApplicationConfig
from silvimetric import __version__ as svversion


@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope="function")
def threaded_dask():
    dask.config.set(scheduler="threads")

@pytest.fixture(scope='function')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs, metrics):
    log = Log(20)
    yield StorageConfig(tdb_dir = tdb_filepath, 
                        log = log,
                        crs = crs,
                        bounds = bounds,
                        resolution = resolution,
                        attrs = attrs,
                        metrics = metrics,
                        version = svversion)

@pytest.fixture(scope='function')
def metrics():
    yield [Metrics['mean'], Metrics['median']]

@pytest.fixture(scope="function")
def storage(storage_config) -> Storage:
    yield Storage.create(storage_config)

@pytest.fixture(scope='function')
def app_config(tdb_filepath, debug=True):
    log = Log(20) # INFO
    app = ApplicationConfig(tdb_dir = tdb_filepath,
                            log = log)
    yield app

@pytest.fixture(scope='function')
def shatter_config(tdb_filepath, copc_filepath, tile_size, storage_config, app_config, storage):
    log = Log(20) # INFO
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log = log,
                      filename = copc_filepath,
                      tile_size = tile_size,
                      attrs = storage_config.attrs,
                      metrics = storage_config.metrics,
                      debug = True)
    yield s

@pytest.fixture(scope='session')
def copc_filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data_2.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='session')
def autzen_filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "autzen-small.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='session')
def pipeline_filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data_2.json")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def bounds(minx, maxx, miny, maxy) -> Bounds:
    yield Bounds(minx, miny, maxx, maxy)

@pytest.fixture(scope='class')
def extents(resolution, tile_size, bounds, crs) -> Extents:
    yield Extents(bounds,resolution,tile_size,crs)

@pytest.fixture(scope="session")
def attrs(dims) -> list[str]:
    yield [Attribute(a, dims[a]) for a in
           ['Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity']]

@pytest.fixture(scope="session")
def dims():
    yield { d['name']: d['dtype'] for d in pdal.dimensions }

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
