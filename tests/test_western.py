import tiledb
import tempfile
import numpy as np
from silvimetric.resources import Storage, StorageConfig, Metrics, Attribute, Bounds

from silvimetric.commands.shatter import shatter
from silvimetric.resources import Storage, Extents, ShatterConfig, Log
from silvimetric import __version__ as svversion
import os, shutil
import pytest

@pytest.fixture(scope="class")
def WebMercator():
    yield "EPSG:3857"

@pytest.fixture(scope='class')
def WesternBounds() -> Bounds:
    b = Bounds(-14100053.268191, 3058230.975702, -11138180.816218, 6368599.176434)
    yield b

@pytest.fixture(scope='function')
def western_filepath(tmp_path_factory):

    temp_name = next(tempfile._get_candidate_names())
    path = tmp_path_factory.mktemp(temp_name)
    yield os.path.abspath(path)
    shutil.rmtree(path)


@pytest.fixture(scope='function')
def western_config(western_filepath, WesternBounds, resolution, WebMercator, attrs, metrics):
    log = Log(20)
    yield StorageConfig(tdb_dir = western_filepath,
                        log = log,
                        crs = WebMercator,
                        root = WesternBounds,
                        resolution = resolution,
                        attrs = attrs,
                        metrics = metrics,
                        version = svversion)

@pytest.fixture(scope='function')
def western_storage(western_config) -> Storage:
    yield Storage.create(western_config)

@pytest.fixture(scope='function')
def western_pipeline():
    path = os.path.join(os.path.dirname(__file__), "data",
            "western_us.json")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def western_shatter_config(western_pipeline, western_storage):
    log = Log(20) # INFO
    st = western_storage.config

    s = ShatterConfig(tdb_dir = st.tdb_dir,
        log = log,
        filename = western_pipeline,
        attrs = st.attrs,
        metrics = st.metrics,
        bounds=bounds,
        debug = True,
        date=date)
    yield s

class Test_Western(object):

    def test_schema(self, western_storage: Storage):
        with western_storage.open('r') as st:
            s:tiledb.ArraySchema = st.schema
            assert s.has_attr('count')
            assert s.attr('count').dtype == np.int32

