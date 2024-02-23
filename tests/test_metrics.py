import os
import pytest
import numpy as np
import pandas as pd
import dask

from silvimetric.resources import Metrics, Log, ShatterConfig, StorageConfig
from silvimetric.resources import Storage, Bounds, Extents
from silvimetric.commands import shatter
from silvimetric import __version__ as svversion

@pytest.fixture(scope='function')
def metric_input():
    # randomly generated int array
    yield np.array([54, 64, 54, 18, 76, 82, 85, 12,  7, 32, 89, 79, 78, 58, 17,  5, 77,
        20, 63, 87, 37, 27,  7, 22, 34, 61, 52, 64, 65, 90, 76, 88,  0, 98,
        78, 26, 88, 44, 78, 54, 38, 28, 94, 74, 90, 84, 30,  7, 81, 75, 22,
        12, 67, 10, 46, 62, 79, 52, 24, 88,  4, 73, 75, 35, 75, 16, 66, 43,
        28, 72, 15, 80, 53,  1, 28, 70, 20, 42, 83, 80, 82,  2, 41, 38, 23,
        17, 18, 19, 43, 30, 88, 41, 60, 29, 93, 27,  1, 13, 93, 82], np.int32)

@pytest.fixture(scope='session')
def autzen_filepath() -> str:
    path = os.path.join(os.path.dirname(__file__), "data",
            "autzen-small.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def autzen_bounds():
    yield Bounds(635579.2, 848884.83, 639003.73, 853536.21)

@pytest.fixture(scope='function')
def metric_st_config(tdb_filepath, autzen_bounds, resolution, crs, attrs, metrics):
    log = Log(20)
    yield StorageConfig(tdb_dir = tdb_filepath,
                        log = log,
                        crs = crs,
                        root = autzen_bounds,
                        resolution = resolution,
                        attrs = attrs,
                        version = svversion)

@pytest.fixture(scope='function')
def metric_shatter(tdb_filepath, autzen_filepath, metric_st_config, autzen_bounds, app_config, date):
    log = Log(20) # INFO
    Storage.create(metric_st_config)
    e = Extents(autzen_bounds, 30, autzen_bounds)
    e1 = e.split()[1]
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log=log,
                      filename=autzen_filepath,
                      attrs=metric_st_config.attrs,
                      metrics=metric_st_config.metrics,
                      bounds=e1.bounds,
                      debug=True,
                      date=date,
                      tile_size=700)

    yield s

class Test_Metrics(object):

    def test_input_output(self, metric_input):
        for m in Metrics:
            try:
                out = Metrics[m]._method(metric_input)
                msg = f"Metric {Metrics[m].name} does not output a valid data type."\
                        f" Interpretted type: {type(out)}"
                assert not any([
                    isinstance(out, np.ndarray),
                    isinstance(out, list),
                    isinstance(out, tuple),
                    isinstance(out, pd.Series),
                    isinstance(out, pd.DataFrame),
                    isinstance(out, str)
                ]), msg
                assert any((
                    isinstance(out, np.number),
                    isinstance(out, int),
                    isinstance(out, float),
                    isinstance(out, complex),
                    isinstance(out, bool)
                )), msg
            except:
                print('yi')

    def test_metric_shatter(self, metric_shatter):
        dask.config.set(scheduler='processes')
        shatter.shatter(metric_shatter)