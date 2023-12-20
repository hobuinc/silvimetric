

from . import Storage
from . import Data
from . import Bounds

import math

class Tile:

    def __init__(self, data: Data, size: int, origin: list[int], storage: Storage, ):
        self.data = data
        self.storage = storage
        self.size = size
        self.x0 = origin[0]
        self.y0 = origin[1]
        self.x1 = origin[0] * size
        self.y1 = origin[1] * size

    def get_bounds(self):
        resolution = self.storage.root.resolution
        minx = self.storage.root.minx + (self.x0 * resolution)
        miny = self.storage.root.miny + (self.y0 * resolution)
        maxx = self.storage.root.minx + (self.x1 * resolution)
        maxy = self.storage.root.miny + (self.y1 * resolution)
        b = Bounds(minx, miny, maxx, maxy)
        return b
    bounds = property(get_bounds)

    def get_count_estimate(self):
        b = self.get_bounds()
        return self.data.estimate_count(b)
    count_estimate = property(get_count_estimate)

    def get_count(self):
        b = self.get_bounds()
        return self.data.count(b)
    count = property(get_count)

    def bisect(self):
        centerx = self.minx + ((self.maxx - self.minx)/ 2)
        centery = self.miny + ((self.maxy - self.miny)/ 2)
        yield from [
            Bounds(self.minx, self.miny, centerx, centery), # lower left
            Bounds(centerx, self.miny, self.maxx, centery), # lower right
            Bounds(self.minx, centery, centerx, self.maxy), # top left
            Bounds(centerx, centery, self.maxx, self.maxy), # top right
        ]

