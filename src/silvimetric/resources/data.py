
from . import Bounds 

import pdal

import pathlib

class Data:

    def __init__(self, filename: str, bounds: Bounds, resolution: float):
        self.filename = filename
        self.bounds = bounds
        self.resolution = resolution
        self.reader = self.get_reader()
        self.root = self.get_root()
        self.pipeline = self.get_pipeline()
        self.reader = self.get_reader()
        self.root = self.get_root()
    
    def is_pipeline(self):
        p = pathlib.Path(self.filename)
        if p.suffix == '.json':
            return True
        return False


    def get_root(self):
        reader = self.get_reader()
        reader._options['threads'] = 2

        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']

        b = qi['bounds']
        return Bounds(b['minx'],
                      b['miny'],
                      b['maxx'],
                      b['maxy'])


    def make_pipeline(self):
        reader = pdal.Reader(self.filename, tag='reader')
        reader._options['threads'] = 2
        reader._options['bounds'] = str(self.bounds)
        class_zero = pdal.Filter.assign(value="Classification = 0")
        rn = pdal.Filter.assign(value="ReturnNumber = 1 WHERE ReturnNumber < 1")
        nor = pdal.Filter.assign(value="NumberOfReturns = 1 WHERE NumberOfReturns < 1")
        ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")

        self.root = self.get_root()
        assign_x = pdal.Filter.assign(
            value=f"xi = (X - {self.root.minx}) / {self.resolution}")
        assign_y = pdal.Filter.assign(
            value=f"yi = ({self.root.maxy} - Y) / {self.resolution}")

        return reader | class_zero | rn | nor | ferry | assign_x | assign_y #| smrf | hag

    def get_pipeline(self):
        if self.is_pipeline():
            p = pathlib.Path(self.filename)
            j = p.read_bytes().decode('utf-8')
            stages = pdal.pipeline._parse_stages(j)
            pipeline = pdal.Pipeline(stages)
        else:
            pipeline = self.make_pipeline()

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
                stage._options['bounds'] = str(self.bounds)

            # we don't support weird pipelines of shapes
            # that aren't simply a line
            if stage_type != 'writers':
                stages.append(stage)

        if len(readers) != 1:
            raise Exception(f"Pipelines can only have one reader of type {allowed_readers}")

        # Add xi and yi
        ferry = pdal.Filter.ferry(dimensions="X=>xi, Y=>yi")
        assign_x = pdal.Filter.assign(value=f"xi = (X - {self.root.minx}) / {self.resolution}")
        assign_y = pdal.Filter.assign(value=f"yi = ({self.root.maxy} - Y) / {self.resolution}")
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

    def get_array(self):
        return self.pipeline.arrays[0]
    array = property(get_array)
    
    def get_reader(self):
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
            reader._options['threads'] = 2
            return reader


    def count(self, bounds: Bounds):
        
        reader = self.get_reader()
        reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        minx, miny, maxx, maxy = self.bounds.get()

        yield pc