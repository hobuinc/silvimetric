import pytest
import os
import dask
import pdal

from silvimetric import Extents, Bounds, Metrics, Attribute, Storage
from silvimetric import ShatterConfig, StorageConfig
from silvimetric import __version__ as svversion

from silvimetric.commands.shatter import create_pipeline

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
    yield StorageConfig(tdb_filepath, bounds, resolution, crs, attrs, metrics,
                        svversion)

@pytest.fixture(scope='function')
def metrics():
    yield [Metrics['mean'], Metrics['median']]

@pytest.fixture(scope="function")
def storage(storage_config) -> Storage:
    yield Storage.create(storage_config)

@pytest.fixture(scope='function')
def shatter_config(tdb_filepath, filepath, tile_size, storage_config, storage):
    yield ShatterConfig(tdb_filepath, filepath, tile_size,
                               storage_config.attrs, storage_config.metrics,
                               debug=True)

@pytest.fixture(scope="function")
def s3_bucket():
    yield "silvimetric"

@pytest.fixture(scope='function')
def s3_uri(s3_bucket):
    yield f"s3://{s3_bucket}/test_copc_shatter"

@pytest.fixture(scope="function")
def s3_storage_config(s3_uri, bounds, resolution, crs, attrs, metrics):
    yield StorageConfig(s3_uri, bounds, resolution, crs, attrs, metrics,
                        svversion)

@pytest.fixture(scope='function')
def s3_storage(s3_storage_config, s3_bucket):
    yield Storage.create(s3_storage_config)

@pytest.fixture(scope="function")
def s3_shatter_config(s3_storage, filepath, attrs, metrics):
    config = s3_storage.config
    yield ShatterConfig(config.tdb_dir, filepath, 200, attrs, metrics, True)

@pytest.fixture(scope='session')
def filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "test_data_2.copc.laz")
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
