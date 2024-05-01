import pytest
import os
import dask
import pdal

from datetime import datetime
from typing import Generator

from silvimetric import Extents, Bounds, Metrics, Attribute, Storage
from silvimetric import Log, Metric, ShatterConfig, StorageConfig
from silvimetric import ApplicationConfig, ExtractConfig
from silvimetric import __version__ as svversion

# pull together fixtures
pytest_plugins=[
    'fixtures.shatter_fixtures', 'fixtures.extract_fixtures',
    'fixtures.command_fixtures', 'fixtures.chunk_fixtures',
    'fixtures.western_fixtures', 'fixtures.data_fixtures',
    'fixtures.cli_fixtures', 'fixtures.fusion_fixtures']

@pytest.fixture(scope="session", autouse=True)
def configure_dask() -> None:
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope="function")
def threaded_dask() -> None:
    dask.config.set(scheduler="threads")

@pytest.fixture(scope='function')
def tdb_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tdb")
    p = os.path.abspath(path)
    yield p

@pytest.fixture(scope='function')
def app_config(tdb_filepath, debug=True) -> Generator[ApplicationConfig, None, None]:
    log = Log(20) # INFO
    app = ApplicationConfig(tdb_dir = tdb_filepath,
                            log = log)
    yield app

@pytest.fixture(scope='function')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs, metrics) -> Generator[StorageConfig, None, None]:
    log = Log('DEBUG')
    yield StorageConfig(tdb_dir = tdb_filepath,
                        log = log,
                        crs = crs,
                        root = bounds,
                        resolution = resolution,
                        attrs = attrs,
                        metrics = metrics,
                        version = svversion)

@pytest.fixture(scope="function")
def storage(storage_config) -> Generator[Storage, None, None]:
    yield Storage.create(storage_config)

@pytest.fixture(scope='function')
def shatter_config(tdb_filepath, copc_filepath, storage_config, bounds,
        app_config, storage, date) -> Generator[ShatterConfig, None, None]:
    log = Log('INFO') # INFO
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log = log,
                      filename = copc_filepath,
                      attrs = storage_config.attrs,
                      metrics = storage_config.metrics,
                      bounds=bounds,
                      debug = True,
                      date=date)

    yield s

@pytest.fixture(scope='function')
def extract_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs, storage):
    from silvimetric.commands import shatter
    shatter.shatter(shatter_config)
    log = Log(20)
    c =  ExtractConfig(tdb_dir = tdb_filepath,
                       log = log,
                       out_dir = tif_filepath,
                       attrs = extract_attrs,
                       metrics = metrics)
    yield c


@pytest.fixture(scope='session')
def metrics() -> Generator[list[Metric], None, None]:
    yield [Metrics['mean'], Metrics['median']]

@pytest.fixture(scope='class')
def bounds(minx, maxx, miny, maxy) -> Generator[Bounds, None, None]:
    b =  Bounds(minx, miny, maxx, maxy)
    yield b

@pytest.fixture(scope='class')
def extents(resolution, bounds) -> Generator[Extents, None, None]:
    yield Extents(bounds,resolution,bounds)

@pytest.fixture(scope="session")
def attrs(dims) -> Generator[list[str], None, None]:
    yield [Attribute(a, dims[a]) for a in
           ['Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity']]

@pytest.fixture(scope="session")
def dims() -> Generator[dict, None, None]:
    yield { d['name']: d['dtype'] for d in pdal.dimensions }

@pytest.fixture(scope='class')
def resolution() -> Generator[int, None, None]:
    yield 30

@pytest.fixture(scope='class')
def test_point_count() -> Generator[int, None, None]:
    yield 90000

@pytest.fixture(scope='class')
def minx() -> Generator[float, None, None]:
    yield 300

@pytest.fixture(scope='class')
def miny() -> Generator[float, None, None]:
    yield 300

@pytest.fixture(scope='class')
def maxx() -> Generator[float, None, None]:
    yield 600

@pytest.fixture(scope='class')
def maxy() -> Generator[float, None, None]:
    yield 600

@pytest.fixture(scope="class")
def crs() -> Generator[str, None, None]:
    yield "EPSG:5070"

@pytest.fixture(scope='class')
def date() -> Generator[datetime, None, None]:
    yield datetime(2011, 1, 1)