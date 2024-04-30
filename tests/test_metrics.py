import pytest
import pandas as pd

class TestMetrics():
    def test_filter(storage_config):

        def test_filter(df: pd.DataFrame):
            assert isinstance(df, pd.DataFrame)
            return pd.where(df['NumberOfReturns'] > 1)