

from . import Storage
from . import Data
from . import Bounds

import math

class Tile:

    def __init__(self, data: Data, storage: Storage, size: int, origin: list[int]):
        self.data = data
        self.storage = storage
        self.size = size
        self.x0 = origin[0]
        self.y0 = origin[1]
        self.x1 = origin[0] * size
        self.y1 = origin[1] * size

        # self.x1 = math.floor((minx - self.root.minx) / resolution)
        # self.y1 = math.floor((self.root.maxy - maxy) / resolution)
        # self.x2 = math.floor((maxx - self.root.minx) / resolution)
        # self.y2 = math.floor((self.root.maxy - miny) / resolution)

    def get_count(self):

        resolution = self.storage.root.resolution
        bminx = self.storage.root.minx
        bminy = self.storage.root.miny
        bmaxx = self.storage.root.maxx
        bmaxy = self.storage.root.maxy

        x_buf = 1 if bmaxx % resolution != 0 else 0
        y_buf = 1 if bmaxy % resolution != 0 else 0

        # make bounds in scale with the desired resolution
        minx = bminx + (self.x0 * self.resolution)
        maxx = bminx + ((self.x2 + x_buf) * self.resolution)
        miny = bmaxy - ((self.y2 + y_buf) * self.resolution)
        maxy = bmaxy - (self.y1 * self.resolution)

        resolution = self.storage.root.resolution
        minx = self.storage.root.minx + (self.x0 * resolution)
        miny = self.storage.root.minx + (self.y0 * resolution)
        b = Bounds(minx, miny, maxx, maxy)

