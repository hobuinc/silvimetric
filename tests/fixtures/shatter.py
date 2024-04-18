import pytest
from typing import Generator
from uuid import uuid4

from silvimetric import __version__ as svversion
from silvimetric.resources import StorageConfig, ShatterConfig, Storage, Log

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

@pytest.fixture(scope='function')
def uneven_storage_config(tdb_filepath, bounds, crs, attrs, metrics) -> Generator[StorageConfig, None, None]:
    log = Log('INFO')
    sc = StorageConfig(tdb_dir = tdb_filepath,
                        log = log,
                        crs = crs,
                        root = bounds,
                        resolution = 7,
                        attrs = attrs,
                        metrics = metrics,
                        version = svversion)
    Storage.create(sc)
    yield sc

@pytest.fixture(scope='function')
def uneven_storage(tdb_filepath) -> Generator[Storage, None, None]:
    yield Storage.from_db(tdb_filepath)

@pytest.fixture(scope='function')
def uneven_shatter_config(tdb_filepath, copc_filepath, uneven_storage_config, date) -> Generator[ShatterConfig, None, None]:
    log = Log('INFO') # INFO
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log = log,
                      filename = copc_filepath,
                      attrs = uneven_storage_config.attrs,
                      metrics = uneven_storage_config.metrics,
                      debug = True,
                      date=date)
    yield s