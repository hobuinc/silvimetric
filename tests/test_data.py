import pytest
import os

from silvimetric.resources import Data
from silvimetric.resources import Storage

import conftest


class Test_Data(object):


    def test_filepath(self, copc_filepath, bounds, storage: Storage, test_point_count):
        """Check open a COPC file"""
        data = Data(copc_filepath, bounds, storage.config)
        assert data.is_pipeline() == False
        data.execute()
        assert len(data.array) == test_point_count

    def test_pipeline(self, pipeline_filepath, bounds, storage: Storage, test_point_count):
        """Check open a pipeline"""
        data = Data(pipeline_filepath, bounds, storage.config)
        assert data.is_pipeline() == True
        data.execute()
        assert len(data.array) == test_point_count
        assert data.count(data.bounds) == test_point_count

