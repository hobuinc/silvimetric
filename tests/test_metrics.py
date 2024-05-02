import os
import pytest
import pandas as pd
import numpy as np

from typing import Generator

from silvimetric import Metric, shatter, ShatterConfig, StorageConfig, Storage, Log
from silvimetric import __version__ as svversion

@pytest.fixture(scope='class')
def metric_shatter_config(tmp_path_factory, copc_filepath, attrs, metrics, bounds,
        date, crs, resolution) -> Generator[pd.Series, None, None]:

    path = tmp_path_factory.mktemp("test_tdb")
    tdb_filepath = os.path.abspath(path)
    log = Log(10)

    def dummy_fn(df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df, pd.DataFrame)
        ndf = df[df['NumberOfReturns'] > 1]
        assert isinstance(ndf, pd.DataFrame)
        return ndf

    metrics[0].add_filter(dummy_fn, 'This is a function.')
    metrics[0].attributes=attrs

    """Make output"""
    st_config=StorageConfig(tdb_dir=tdb_filepath,
                        log=log,
                        crs=crs,
                        root=bounds,
                        resolution=resolution,
                        attrs=attrs,
                        metrics=metrics,
                        version=svversion)

    s = Storage.create(st_config)
    sh_config = ShatterConfig(tdb_dir=tdb_filepath,
            log=log,
            filename=copc_filepath,
            bounds=bounds,
            debug=True,
            date=date)
    yield sh_config

class TestMetrics():
    def test_filter(self, metric_shatter_config, test_point_count):

        m = metric_shatter_config.metrics[0]
        assert len(m.filters) == 1

        pc = shatter(metric_shatter_config)
        assert pc == test_point_count

        s = Storage.from_db(metric_shatter_config.tdb_dir)
        with s.open('r') as a:
            q = a.query(coords=False, use_arrow=False).df

            nor_mean = q[:]['m_NumberOfReturns_mean']
            nor = q[:]['NumberOfReturns']
            assert not nor_mean.isna().all()
            assert nor.notna().all()

            where_nan = nor_mean[nor_mean.isna()]
            assert where_nan.size == 20
