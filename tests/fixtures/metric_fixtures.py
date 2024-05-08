import os
import pytest
from typing import Generator
import pandas as pd

from silvimetric import Log, StorageConfig, ShatterConfig, Storage
from silvimetric import __version__ as svversion

@pytest.fixture(scope='function')
def metric_shatter_config(tdb_filepath, copc_filepath, attrs, metrics, bounds,
        date, crs, resolution) -> Generator[pd.Series, None, None]:

    log = Log(10)

    def dummy_fn(df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df, pd.DataFrame)
        ndf = df[df['NumberOfReturns'] >= 1]
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