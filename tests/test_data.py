import pytest
import os

from silvimetric.resources import Data
from silvimetric.resources import Storage
from silvimetric.resources import Bounds

import conftest


class Test_Data(object):

    def test_filepath(self, copc_filepath, storage: Storage, test_point_count, bounds):
        """Check open a COPC file"""
        data = Data(copc_filepath, storage.config)
        assert data.is_pipeline() == False
        data.execute()
        assert len(data.array) == test_point_count
        assert data.estimate_count(bounds) == test_point_count

    def test_pipeline(self, pipeline_filepath, bounds, storage: Storage, test_point_count):
        """Check open a pipeline"""
        data = Data(pipeline_filepath, storage.config)
        assert data.is_pipeline() == True
        data.execute()
        assert len(data.array) == test_point_count
        assert data.estimate_count(bounds) == test_point_count

    def test_pipeline_bounds(self, pipeline_filepath, bounds, storage: Storage, test_point_count):
        """Check open a pipeline with our own bounds"""
        ll = list(bounds.bisect())[0]
        data = Data(pipeline_filepath, storage.config, bounds = ll)
        assert data.is_pipeline() == True
        data.execute()
        assert len(data.array) == test_point_count / 4
        assert data.estimate_count(ll) == test_point_count 
        assert data.count(ll) == test_point_count / 4 


class Test_Autzen(object):

    def test_filepath(self, autzen_filepath, storage: Storage, test_point_count, bounds):
        """Check open Autzen """
        data = Data(autzen_filepath, storage.config)
        assert data.is_pipeline() == False
        data.execute()
        assert len(data.array) == 577637
        assert data.estimate_count(data.bounds) == 577637
