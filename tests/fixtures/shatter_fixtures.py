from datetime import datetime
import pytest
from typing_extensions import Generator
from uuid import uuid4
import os

from silvimetric import __version__ as svversion
from silvimetric import StorageConfig, ShatterConfig, Storage, Log, Bounds
from silvimetric.resources.attribute import Attribute
from silvimetric.resources.metric import Metric

@pytest.fixture(scope='session')
def s3_copc_filepath() -> Generator[str, None, None]:
    path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'test_data_pixel_point.copc.laz',
    )
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def s3_bucket() -> Generator[str, None, None]:
    yield 'silvimetric'


@pytest.fixture(scope='function')
def s3_uri(s3_bucket: str) -> Generator[str, None, None]:
    uuid = uuid4()
    yield f's3://{s3_bucket}/test_silvimetric/{uuid}'


@pytest.fixture(scope='function')
def s3_storage_config(
    s3_uri: str,
    bounds: Bounds,
    resolution: int,
    crs: str,
    attrs: list[Attribute],
    metrics: list[Metric],
) -> Generator[StorageConfig, None, None]:
    yield StorageConfig(
        root=bounds,
        crs=crs,
        resolution=resolution,
        attrs=attrs,
        metrics=metrics,
        version=svversion,
        tdb_dir=s3_uri,
    )


@pytest.fixture(scope='function')
def s3_storage(
    s3_storage_config: StorageConfig,
) -> Generator[Storage, None, None]:
    import subprocess

    yield Storage.create(s3_storage_config)
    subprocess.call(
        ['aws', 's3', 'rm', '--recursive', s3_storage_config.tdb_dir]
    )


@pytest.fixture(scope='function')
def s3_shatter_config(
    s3_storage: Storage,
    s3_copc_filepath: str,
    date: datetime,
) -> Generator[ShatterConfig, None, None]:
    config = s3_storage.config
    yield ShatterConfig(
        filename=s3_copc_filepath, tdb_dir=config.tdb_dir, date=date
    )


@pytest.fixture(scope='function')
def uneven_storage_config(
    tmp_path_factory: pytest.TempPathFactory,
    bounds: Bounds,
    crs: str,
    attrs: list[Attribute],
    metrics: list[Metric],
) -> Generator[StorageConfig, None, None]:
    log = Log('INFO')
    path = tmp_path_factory.mktemp('test_tdb')
    p = os.path.abspath(path)

    sc = StorageConfig(
        tdb_dir=p,
        log=log,
        crs=crs,
        root=bounds,
        resolution=7,
        attrs=attrs,
        metrics=metrics,
        version=svversion,
        xsize = 2,
        ysize = 2
    )
    Storage.create(sc)
    yield sc


@pytest.fixture(scope='function')
def uneven_shatter_config(
    copc_filepath: str, uneven_storage_config: StorageConfig, date: datetime
) -> Generator[ShatterConfig, None, None]:
    tdb_dir = uneven_storage_config.tdb_dir
    log = uneven_storage_config.log

    s = ShatterConfig(
        tdb_dir=tdb_dir, log=log, filename=copc_filepath, date=date
    )
    yield s


@pytest.fixture(scope='function')
def partial_storage_config(
    tmp_path_factory: pytest.TempPathFactory,
    crs: str,
    attrs: list[Attribute],
    metrics: list[Metric],
    bounds: Bounds,
    alignment: int,
) -> Generator[StorageConfig, None, None]:
    path = tmp_path_factory.mktemp('test_tdb')
    p = os.path.abspath(path)
    log = Log('INFO')

    b = next(iter(bounds.bisect()))
    sc = StorageConfig(
        tdb_dir=p,
        log=log,
        crs=crs,
        root=b,
        resolution=30,
        attrs=attrs,
        metrics=metrics,
        alignment=alignment,
        version=svversion,
        xsize=5,
        ysize=5
    )
    Storage.create(sc)
    yield sc


@pytest.fixture(scope='function')
def partial_shatter_config(
    copc_filepath: str, date: datetime, partial_storage_config: StorageConfig
) -> Generator[ShatterConfig, None, None]:
    tdb_dir = partial_storage_config.tdb_dir
    log = Log('INFO')  # INFO
    yield ShatterConfig(
        tdb_dir=tdb_dir, log=log, filename=copc_filepath, date=date
    )
