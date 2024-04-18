import pytest
import os
import copy
import uuid
from typing import Generator

from silvimetric.commands.shatter import shatter
from silvimetric.resources import Attribute, ExtractConfig, Extents, Log

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs(dims)->Generator[list[str], None, None]:
    yield [Attribute('Z', dtype=dims['Z']), Attribute('Intensity', dtype=dims['Intensity'])]


@pytest.fixture(scope='function')
def multivalue_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs):

    shatter(shatter_config)
    e = Extents.from_storage(shatter_config.tdb_dir)
    b: Extents = e.split()[0]

    second_config = copy.deepcopy(shatter_config)
    second_config.bounds = b.bounds
    second_config.name = uuid.uuid4()
    second_config.point_count = 0

    shatter(second_config)
    log = Log(20)
    c =  ExtractConfig(tdb_dir = tdb_filepath,
                       log = log,
                       out_dir = tif_filepath,
                       attrs = extract_attrs,
                       metrics = metrics)
    yield c
