from datetime import datetime
import tempfile
from typing_extensions import Generator

from silvimetric import Storage, ShatterConfig, Log, Bounds, StorageConfig
from silvimetric import __version__ as svversion
import os
import shutil
import pytest

from silvimetric.resources.attribute import Attribute
from silvimetric.resources.metric import Metric


@pytest.fixture(scope='class')
def WebMercator():
    yield 'EPSG:3857'


@pytest.fixture(scope='class')
def WesternBounds() -> Generator[Bounds, None, None]:
    b = Bounds(
        -14100053.268191, 3058230.975702, -11138180.816218, 6368599.176434
    )
    yield b


@pytest.fixture(scope='function')
def western_filepath(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[str, None, None]:
    temp_name = next(tempfile._get_candidate_names())
    path = tmp_path_factory.mktemp(temp_name)
    yield os.path.abspath(path)
    shutil.rmtree(path)


@pytest.fixture(scope='function')
def western_config(
    western_filepath: str,
    WesternBounds: Bounds,
    resolution: int,
    WebMercator: str,
    attrs: list[Attribute],
    metrics: list[Metric],
) -> Generator[StorageConfig, None, None]:
    log = Log(20)
    yield StorageConfig(
        tdb_dir=western_filepath,
        log=log,
        crs=WebMercator,
        root=WesternBounds,
        resolution=resolution,
        attrs=attrs,
        metrics=metrics,
        version=svversion,
    )


@pytest.fixture(scope='function')
def western_storage(
    western_config: StorageConfig,
) -> Generator[Storage, None, None]:
    yield Storage.create(western_config)


@pytest.fixture(scope='function')
def western_pipeline() -> Generator[str, None, None]:
    path = os.path.join(os.path.dirname(__file__), 'data', 'western_us.json')
    assert os.path.exists(path)
    yield os.path.abspath(path)


@pytest.fixture(scope='function')
def western_shatter_config(
    western_pipeline: str,
    western_storage: Storage,
    bounds: Bounds,
    date: datetime,
) -> Generator[ShatterConfig, None, None]:
    log = Log(20)  # INFO
    st = western_storage.config

    s = ShatterConfig(
        tdb_dir=st.tdb_dir,
        log=log,
        filename=western_pipeline,
        bounds=bounds,
        date=date,
    )
    yield s
