import pytest

from silvimetric import Data, Bounds, Extents
from silvimetric.resources.config import StorageConfig
import dask.bag as db
from dask.diagnostics import ProgressBar


class Test_Data(object):
    def test_filepath(
        self,
        no_cell_line_path: str,
        storage_config: StorageConfig,
        no_cell_line_pc: int,
        bounds: Bounds,
    ):
        """Check open a COPC file"""
        data = Data(no_cell_line_path, storage_config)
        assert not data.is_pipeline()
        data.execute()
        assert len(data.array) == no_cell_line_pc
        assert data.estimate_count(bounds) == no_cell_line_pc

    def test_pipeline(
        self,
        no_cell_line_pipeline: str,
        bounds: Bounds,
        storage_config: StorageConfig,
        no_cell_line_pc: int,
    ):
        """Check open a pipeline"""
        data = Data(no_cell_line_pipeline, storage_config)
        assert data.is_pipeline()
        data.execute()
        assert len(data.array) == no_cell_line_pc
        assert data.estimate_count(bounds) == no_cell_line_pc

    def test_pipeline_bounds(
        self,
        no_cell_line_pipeline: str,
        bounds: Bounds,
        storage_config: StorageConfig,
        no_cell_line_pc: int,
    ):
        """Check open a pipeline with our own bounds"""
        ll = next(iter(bounds.bisect()))

        data = Data(no_cell_line_pipeline, storage_config, bounds=ll)

        # data will be collared upon execution, extra data will be grabbed

        minx, miny, maxx, maxy = data.bounds.get()
        collared = Bounds(minx - 30, miny - 30, maxx + 30, maxy + 30)

        assert data.is_pipeline()
        data.execute()

        collared_count = data.count(collared)
        assert len(data.array) == collared_count

        assert data.estimate_count(ll) == no_cell_line_pc
        correct_count = (
            21025
            if storage_config.alignment.lower() == 'aligntocorner'
            else 25600
        )
        assert data.count(ll) == correct_count


class Test_Autzen(object):
    def test_filepath(
        self, autzen_filepath: str, storage_config: StorageConfig
    ):
        """Check open Autzen"""
        data = Data(autzen_filepath, storage_config)
        assert not data.is_pipeline()
        data.execute()
        assert len(data.array) == 577637
        assert data.estimate_count(data.bounds) == 577637

    @pytest.mark.skip()
    def test_chunking(
        self, autzen_data: Data, autzen_storage: StorageConfig, threaded_dask
    ):
        ex = Extents(
            autzen_data.bounds,
            autzen_storage.resolution,
            autzen_storage.alignment,
            autzen_storage.root,
        )
        chs = ex.chunk(autzen_data, pc_threshold=600000)
        with ProgressBar():
            leaves = db.from_sequence(chs)

        assert leaves.npartitions == 62
