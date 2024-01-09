
from . import Bounds
from .config import StorageConfig
import numpy as np

import pdal

import pathlib


class Data:

    def __init__(self,
                 filename: str,
                 storageconfig: StorageConfig,
                 bounds: Bounds = None):
        self.filename = filename
        self.bounds = bounds

        self.reader_thread_count = 2

        self.storageconfig = storageconfig
        self.reader = self.get_reader()
        self.pipeline = self.get_pipeline()


    def is_pipeline(self) -> bool:
        """Does this instance represent a pdal.Pipeline or a simple filename"""

        p = pathlib.Path(self.filename)
        if p.suffix == '.json':
            return True
        return False


    def make_pipeline(self) -> pdal.Pipeline:
        """Take a COPC or EPT endpoint and generate a PDAL pipeline for it"""

        reader = pdal.Reader(self.filename, tag='reader')
        reader._options['threads'] = self.reader_thread_count
        if self.bounds:
            reader._options['bounds'] = str(self.bounds)
        class_zero = pdal.Filter.assign(value="Classification = 0")
        rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
        nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
        ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")

        # assign_x = pdal.Filter.assign(
        #     value=f"xi = (X - {self.root.minx}) / {resolution}")
        # assign_y = pdal.Filter.assign(
        #     value=f"yi = ({self.root.maxy} - Y) / {resolution}")

        return reader | class_zero | rn | nor #| ferry | assign_x | assign_y #| smrf | hag

    def get_pipeline(self):
        """Fetch the pipeline for the instance"""

        # If we are a pipeline, read and parse it. If we
        # aren't, go make_pipeline using some options that
        # process the data
        if self.is_pipeline():
            p = pathlib.Path(self.filename)
            j = p.read_bytes().decode('utf-8')
            stages = pdal.pipeline._parse_stages(j)
            pipeline = pdal.Pipeline(stages)
        else:
            pipeline = self.make_pipeline()

        # only support COPC or EPT if someone gave us a pipeline
        # because we need to use bounds-accelerated reads to
        # process data quickly
        allowed_readers = ['copc', 'ept']
        readers = []
        stages = []

        for stage in pipeline.stages:
            stage_type, stage_kind = stage.type.split('.')
            if stage_type == 'readers':
                if not stage_kind in allowed_readers:
                    raise Exception(f"Readers for SilviMetric must be of type 'copc' or 'ept', not '{stage_kind}'")
                readers.append(stage)

            # we only answer to copc or ept readers
            if stage_kind in allowed_readers:
                if self.bounds:
                    stage._options['bounds'] = str(self.bounds)

            # We strip off any writers from the pipeline that were
            # given to us and drop them  on the floor
            if stage_type != 'writers':
                stages.append(stage)

        # we don't support weird pipelines of shapes
        # that aren't simply a line.
        if len(readers) != 1:
            raise Exception(f"Pipelines can only have one reader of type {allowed_readers}")

        resolution = self.storageconfig.resolution
        # Add xi and yi – only need this for PDAL < 2.6
        ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")
        assign_x = pdal.Filter.assign(value=f"xi = (X - {self.storageconfig.root.minx}) / {resolution}")
        assign_y = pdal.Filter.assign(value=f"yi = ({self.storageconfig.root.maxy} - Y) / {resolution}")
        stages.append(ferry)
        stages.append(assign_x)
        stages.append(assign_y)

        # return our pipeline
        return pdal.Pipeline(stages)

    def execute(self):
        try:
            self.pipeline.execute()
        except Exception as e:
            print(self.pipeline.pipeline, e)
            raise e

    def get_array(self) -> np.ndarray:
        """Fetch the array from the execute()'d pipeline"""
        return self.pipeline.arrays[0]
    array = property(get_array)

    def get_reader(self) -> pdal.Reader:
        """Grab or make the reader for this instance so we can use it to do things like
        get the count()"""
        if self.is_pipeline():
            p = pathlib.Path(self.filename)
            j = p.read_bytes().decode('utf-8')
            stages = pdal.pipeline._parse_stages(j)
            pipeline = pdal.Pipeline(stages)
            for stage in pipeline.stages:
                stage_type, stage_kind = stage.type.split('.')
                if stage_type == 'readers':
                    return stage
        else:
            reader = pdal.Reader(self.filename)
            reader._options['threads'] = self.reader_thread_count
            return reader


    def estimate_count(self, bounds: Bounds) -> int:
        """For the provided bounds, estimate the maximum number of points that could be inside them for this instance."""
        reader = self.get_reader()
        if bounds:
            reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']

        return pc

    def count(self, bounds: Bounds) -> int:
        """For the provided bounds, read and count the number of points that are inside them for this instance."""
        reader = self.get_reader()
        if bounds:
            reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()
        pipeline.execute()
        return len(pipeline.arrays[0])
