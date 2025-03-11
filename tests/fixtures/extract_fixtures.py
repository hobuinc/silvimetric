import pytest
import os
import copy
import uuid
from typing_extensions import Generator
import pandas as pd

from silvimetric.commands.shatter import shatter
from silvimetric import Attribute, Log, Bounds, Storage
from silvimetric import ShatterConfig, StorageConfig, ExtractConfig
from silvimetric import __version__ as svversion

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs(dims)->Generator[list[str], None, None]:
    yield [Attribute('Z', dtype=dims['Z']), Attribute('Intensity', dtype=dims['Intensity'])]

@pytest.fixture(scope='function')
def extract_shatter_config(tmp_path_factory, copc_filepath, attrs, metrics, bounds,
        date, crs, resolution) -> Generator[pd.Series, None, None]:

    path = tmp_path_factory.mktemp("test_tdb")
    p = os.path.abspath(path)
    log = Log('DEBUG')

    """Make output"""
    st_config=StorageConfig(tdb_dir=p,
                        log=log,
                        crs=crs,
                        root=bounds,
                        resolution=resolution,
                        attrs=attrs,
                        metrics=metrics,
                        version=svversion)

    s = Storage.create(st_config)
    sh_config = ShatterConfig(tdb_dir=p,
            log=log,
            filename=copc_filepath,
            bounds=bounds,
            debug=True,
            date=date)
    yield sh_config

@pytest.fixture(scope='function')
def multivalue_config(tif_filepath, extract_shatter_config, alignment, resolution):

    shatter(extract_shatter_config)

    ll = list(extract_shatter_config.bounds.bisect())[0]
    ll.adjust_alignment(resolution, alignment)

    # reset config
    second_config = copy.deepcopy(extract_shatter_config)
    second_config.name = uuid.uuid4()
    second_config.bounds = ll
    second_config.point_count = 0
    second_config.time_slot += 1
    second_config.mbr = ()

    shatter(second_config)
    log = Log(20)
    tdb_dir = extract_shatter_config.tdb_dir
    c =  ExtractConfig(tdb_dir = tdb_dir,
                       log = log,
                       out_dir = tif_filepath)
    yield c
