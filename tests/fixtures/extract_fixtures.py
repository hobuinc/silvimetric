import pytest
import os
import copy
import uuid
from typing_extensions import Generator

from silvimetric.commands.shatter import shatter
from silvimetric import Attribute, ExtractConfig, Log
from pandas import Series


@pytest.fixture(scope='function')
def tif_filepath(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp('test_tifs')
    yield os.path.abspath(path)


@pytest.fixture(scope='function')
def extract_attrs(dims: dict) -> Generator[list[str], None, None]:
    yield [
        Attribute('Z', dtype=dims['Z']),
        Attribute('Intensity', dtype=dims['Intensity']),
    ]


@pytest.fixture(scope='function')
def multivalue_config(
    tif_filepath: str,
    metric_shatter_config: Series,
    alignment: int,
    resolution: int,
):
    shatter(metric_shatter_config)

    ll = next(iter(metric_shatter_config.bounds.bisect()))
    ll.adjust_alignment(resolution, alignment)

    # reset config
    second_config = copy.deepcopy(metric_shatter_config)
    second_config.name = uuid.uuid4()
    second_config.bounds = ll
    second_config.point_count = 0
    second_config.time_slot += 1
    second_config.mbr = ()

    shatter(second_config)
    log = Log(20)
    tdb_dir = metric_shatter_config.tdb_dir
    c = ExtractConfig(tdb_dir=tdb_dir, log=log, out_dir=tif_filepath)
    yield c
