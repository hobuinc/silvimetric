import math

import dask

import numpy as np
import dask.array as da
from pyproj import CRS
from shapely import from_wkt

class Bounds(object):
    def __init__(self, minx, miny, maxx, maxy, cell_size, group_size = 3, srs=None):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)
        if not srs:
            raise Exception("Missing SRS for bounds")
        self.srs = CRS.from_user_input(srs)
        if self.srs.is_geographic:
            raise Exception(f"Bounds SRS({srs}) is geographic.")
        self.epsg = self.srs.to_epsg()

        self.rangex = self.maxx - self.minx
        self.rangey = self.maxy - self.miny
        self.xi = math.ceil(self.rangex / cell_size)
        self.yi = math.ceil(self.rangey / cell_size)
        self.cell_size = cell_size
        self.group_size = group_size

    def chunk(self, filename:str):
        from .chunk import Chunk
        c = Chunk(self.minx, self.maxx, self.miny, self.maxy, self)
        # c.set_leaves()
        for child_list in dask.compute(c.filter(filename), traverse=True):
            for child in child_list:
                yield child

    def __repr__(self):
        if self.srs:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}]) / EPSG:{self.epsg}"
        else:
            return f"([{self.minx:.2f},{self.maxx:.2f}],[{self.miny:.2f},{self.maxy:.2f}])"

def create_bounds(reader, cell_size, group_size, polygon=None) -> Bounds:
    # grab our bounds
    if polygon:
        p = from_wkt(polygon)
        if not p.is_valid:
            raise Exception("Invalid polygon entered")

        b = p.bounds
        minx = b[0]
        miny = b[1]
        if len(b) == 4:
            maxx = b[2]
            maxy = b[3]
        elif len(b) == 6:
            maxx = b[3]
            maxy = b[4]
        else:
            raise Exception("Invalid bounds found.")

        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        srs = qi['srs']['wkt']
        if not srs:
            raise Exception("No SRS found in data.")

        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                         group_size=group_size, srs=srs)

        reader._options['bounds'] = str(bounds)
        pipeline = reader.pipeline()

    else:
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']
        srs = qi['srs']['wkt']
        if not srs:
            raise Exception("No SRS found in data.")

        bbox = qi['bounds']
        minx = bbox['minx']
        maxx = bbox['maxx']
        miny = bbox['miny']
        maxy = bbox['maxy']
        bounds = Bounds(minx, miny, maxx, maxy, cell_size=cell_size,
                    group_size=group_size, srs=srs)


    if not pc:
        raise Exception("No points found.")
    print("Points found",  pc)


    return bounds