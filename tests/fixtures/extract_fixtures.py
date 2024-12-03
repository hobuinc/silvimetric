import pytest
import os
import copy
import uuid
from typing_extensions import Generator

from silvimetric.commands.shatter import shatter
from silvimetric import Attribute, ExtractConfig, Log, Bounds

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs(dims)->Generator[list[str], None, None]:
    yield [Attribute('Z', dtype=dims['Z']), Attribute('Intensity', dtype=dims['Intensity'])]


@pytest.fixture(scope='function')
def multivalue_config(tif_filepath, metric_shatter_config):

    shatter(metric_shatter_config)

    # reset config
    second_config = copy.deepcopy(metric_shatter_config)
    second_config.name = uuid.uuid4()
    second_config.bounds = Bounds(300,300,450,450)
    second_config.point_count = 0
    second_config.time_slot = 0
    second_config.mbr = ()

    shatter(second_config)
    log = Log(20)
    tdb_dir = metric_shatter_config.tdb_dir
    c =  ExtractConfig(tdb_dir = tdb_dir,
                       log = log,
                       out_dir = tif_filepath)
    yield c
