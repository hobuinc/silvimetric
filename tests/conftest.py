import pytest
import os
import dask
import pdal
from uuid import uuid4
from datetime import datetime

from typing import Generator

from silvimetric.resources import Extents, Bounds, Metrics, Attribute, Storage
from silvimetric.resources import Log, Data, Metric
from silvimetric.resources.config import ShatterConfig, StorageConfig, ApplicationConfig
from silvimetric import __version__ as svversion


@pytest.fixture(scope="session", autouse=True)
def configure_dask() -> None:
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope='function')
def tdb_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope="function")
def threaded_dask() -> None:
    dask.config.set(scheduler="threads")

@pytest.fixture(scope='function')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs, metrics) -> Generator[StorageConfig, None, None]:
    log = Log(20)
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
def app_config(tdb_filepath, debug=True) -> Generator[ApplicationConfig, None, None]:
    log = Log(20) # INFO
    app = ApplicationConfig(tdb_dir = tdb_filepath,
                            log = log)
    yield app

@pytest.fixture(scope='function')
def shatter_config(tdb_filepath, copc_filepath, storage_config, bounds,
        app_config, storage, date) -> Generator[ShatterConfig, None, None]:
    log = Log(20) # INFO
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
def uneven_storage_config(tdb_filepath, bounds, crs, attrs, metrics) -> Generator[StorageConfig, None, None]:
    log = Log(20)
    yield StorageConfig(tdb_dir = tdb_filepath,
                        log = log,
                        crs = crs,
                        root = bounds,
                        resolution = 7,
                        attrs = attrs,
                        metrics = metrics,
                        version = svversion)

@pytest.fixture(scope='function')
def uneven_storage(uneven_storage_config) -> Generator[Storage, None, None]:
    yield Storage.create(uneven_storage_config)

@pytest.fixture(scope='function')
def uneven_shatter_config(tdb_filepath, copc_filepath, uneven_storage_config, storage, date) -> Generator[ShatterConfig, None, None]:
    log = Log(20) # INFO
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log = log,
                      filename = copc_filepath,
                      attrs = uneven_storage_config.attrs,
                      metrics = uneven_storage_config.metrics,
                      debug = True,
                      date=date)
    yield s

@pytest.fixture(scope="function")
def s3_bucket() -> Generator[str, None, None]:
    yield "silvimetric"

@pytest.fixture(scope='function')
def s3_uri(s3_bucket) -> Generator[str, None, None]:
    uuid = uuid4()
    yield f"s3://{s3_bucket}/test_silvimetric/{uuid}"

@pytest.fixture(scope="function")
def s3_storage_config(s3_uri, bounds, resolution, crs, attrs, metrics) -> Generator[StorageConfig, None, None]:
    yield StorageConfig(bounds, crs, resolution, attrs, metrics,
                        svversion, tdb_dir=s3_uri)

@pytest.fixture(scope='function')
def s3_storage(s3_storage_config) -> Generator[Storage, None, None]:
    import subprocess
    yield Storage.create(s3_storage_config)
    subprocess.call(["aws", "s3", "rm", "--recursive", s3_storage_config.tdb_dir])

@pytest.fixture(scope="function")
def s3_shatter_config(s3_storage, copc_filepath, attrs, metrics, date) -> Generator[ShatterConfig, None, None]:
    config = s3_storage.config
    yield ShatterConfig(filename=copc_filepath, attrs=attrs, metrics=metrics,
                        debug=True, tdb_dir=config.tdb_dir, date=date)

@pytest.fixture(scope='session')
def metrics() -> Generator[list[Metric], None, None]:
    yield [Metrics['mean'], Metrics['median']]

@pytest.fixture(scope='session')
def copc_filepath() -> Generator[str, None, None]:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def copc_data(copc_filepath, storage_config) -> Generator[Data, None, None]:
    d = Data(copc_filepath, storage_config)
    yield d

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