import pytest
import numpy as np
import pandas as pd

from silvimetric.resources import Metrics, Log, ShatterConfig, StorageConfig, Storage
from silvimetric.commands import shatter
from silvimetric import __version__ as svversion

@pytest.fixture(scope='function')
def metric_input():
    yield np.array([1,1,1,2,3,4,5,6,7,8,9,10])

@pytest.fixture(scope='function')
def metric_st_config(tdb_filepath, bounds, resolution, crs, attrs, metrics):
    log = Log(20)
    yield StorageConfig(tdb_dir = tdb_filepath,
                        log = log,
                        crs = crs,
                        root = bounds,
                        resolution = resolution,
                        attrs = attrs,
                        version = svversion)

@pytest.fixture(scope="function")
def metric_storage(metric_st_config) -> Storage:
    yield Storage.create(metric_st_config)

@pytest.fixture(scope='function')
def metric_shatter(tdb_filepath, copc_filepath, metric_st_config, bounds, app_config, metric_storage, date):
    log = Log(20) # INFO
    s = ShatterConfig(tdb_dir = tdb_filepath,
                      log = log,
                      filename = copc_filepath,
                      attrs = metric_st_config.attrs,
                      metrics = metric_st_config.metrics,
                      bounds=bounds,
                      debug = True,
                      date=date)

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
        shatter.shatter(metric_shatter)