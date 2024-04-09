from silvimetric.resources import Data
from silvimetric.resources import Storage

class Test_Data(object):

    def test_filepath(self, no_cell_line_path, storage: Storage, no_cell_line_pc, bounds):
        """Check open a COPC file"""
        data = Data(no_cell_line_path, storage.config)
        assert data.is_pipeline() == False
        data.execute()
        assert len(data.array) == no_cell_line_pc
        assert data.estimate_count(bounds) == no_cell_line_pc

    def test_pipeline(self, no_cell_line_pipeline, bounds, storage: Storage, no_cell_line_pc):
        """Check open a pipeline"""
        data = Data(no_cell_line_pipeline, storage.config)
        assert data.is_pipeline() == True
        data.execute()
        assert len(data.array) == no_cell_line_pc
        assert data.estimate_count(bounds) == no_cell_line_pc

    def test_pipeline_bounds(self, no_cell_line_pipeline, bounds, storage: Storage, no_cell_line_pc):
        """Check open a pipeline with our own bounds"""
        ll = list(bounds.bisect())[0]
        data = Data(no_cell_line_pipeline, storage.config, bounds = ll)
        assert data.is_pipeline() == True
        data.execute()
        assert len(data.array) == no_cell_line_pc / 4
        assert data.estimate_count(ll) == no_cell_line_pc
        assert data.count(ll) == no_cell_line_pc / 4

class Test_Autzen(object):

    def test_filepath(self, autzen_filepath, storage: Storage):
        """Check open Autzen """
        data = Data(autzen_filepath, storage.config)
        assert data.is_pipeline() == False
        data.execute()
        assert len(data.array) == 577637
        assert data.estimate_count(data.bounds) == 577637
