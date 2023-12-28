import pytest

from silvimetric.resources.extents import Tile
from silvimetric.resources import Bounds

import conftest


class Test_Tile:

    def test_creation(self, storage):
        """Test Tile creation"""

        tile = Tile(0, 0, 16, storage)
        assert tile.size == 16

    def test_bounds(self, storage):
        """Do we compute correct bounds information for a tile and resolution"""

        tilesize = 16
        x = 2
        y = 4
        tile = Tile(x, y, tilesize, storage)

        resolution = storage.config.resolution

        x1 = (tilesize  * (x))
        y1 = (tilesize  * (y))

        bounds = tile.bounds
        root = tile.storage.config.root
        assert bounds.minx == root.minx + x1 * resolution
        assert bounds.miny == root.miny + y1 * resolution

        x2 = (tilesize  * (x+1))
        y2 = (tilesize  * (y+1))

        assert bounds.maxx == root.minx + x2 * resolution
        assert bounds.maxy == root.miny + y2 * resolution

        assert tile.x1 == x1
        assert tile.x2 == x2

        assert tile.y1 == y1
        assert tile.y2 == y2