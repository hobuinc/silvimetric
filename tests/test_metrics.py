import os
import pytest
import pandas as pd
import numpy as np
from typing import Generator

from silvimetric import Metric

@pytest.fixture(scope='session')
def df() -> Generator[pd.Series, None, None]:
    path = os.path.join(os.path.dirname(__file__), "data", "df_sample")
    df = pd.read_pickle(path)
    df = df.set_index(['X','Y'])
    yield pd.Series(df.loc[0,423].loc[0,423])

class TestMetrics():
    def test_filter(self, metrics: list[Metric], df: pd.DataFrame):
        m = metrics[0]

        def dummy_fn(df: pd.DataFrame) -> pd.DataFrame:
            assert isinstance(df, pd.DataFrame)
            return df.where(df['NumberOfReturns'] > 1)

        m.add_filter(dummy_fn, 'This is a function that does dummy stuff.')
        assert len(m.filters) == 1

        res = m.run_filters(df)
        print(res)



