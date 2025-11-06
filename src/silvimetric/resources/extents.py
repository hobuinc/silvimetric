import math
import numpy as np
import itertools
from typing import Self

from dask.delayed import delayed
from dask import compute
import copy

from .bounds import Bounds
from .storage import Storage
from .data import Data

IndexDomain = tuple[float, float]
IndexDomainList = tuple[IndexDomain, IndexDomain]


class Extents(object):
    """Handles bounds operations for point cloud data."""

    def __init__(
        self, bounds: Bounds, resolution: float, alignment: str, root: Bounds
    ):
        self.bounds = bounds
        """Bounding box of this section of data."""
        self.root = root
        """Root bounding box of the database."""

        # adjust bounds so they're matching up with cell lines or cell centers
        self.bounds.adjust_alignment(resolution, alignment)
        minx, miny, maxx, maxy = self.bounds.get()

        self.rangex = maxx - minx
        """Range of X Indices"""
        self.rangey = maxy - miny
        """Range of Y indices"""
        self.resolution = resolution
        """Resolution of database."""
        self.alignment = alignment
        """Alignment of pixels in database."""
        self.cell_count = int((self.rangex * self.rangey) / self.resolution**2)
        """Number of cells in this Extents"""
        # TODO add checks for values being outside the bounds. Either throw or
        # adjust the bounds and emit a warning.

        self.x1 = math.floor((minx - self.root.minx) / resolution)
        """Minimum X index"""
        self.x2 = math.ceil((maxx - self.root.minx) / resolution)
        """Maximum X index"""

        self.y1 = math.ceil((self.root.maxy - maxy) / resolution)
        """Minimum Y index, or maximum Y value in point cloud"""
        self.y2 = math.ceil((self.root.maxy - miny) / resolution)
        """Maximum Y index, or minimum Y value in point cloud"""

        self.domain: IndexDomainList = ((self.x1, self.x2), (self.y1, self.y2))
        """Minimum bounding rectangle of this Extents"""

    def get_indices(self):
        """
        Create indices for this section of the database relative to the root
        bounds.

        :return: Indices of this bounding box
        """
        xis = np.arange(self.x1, self.x2, dtype=np.int32)
        yis = np.arange(self.y1, self.y2, dtype=np.int32)
        xys = np.array(np.meshgrid(xis, yis)).T.reshape(-1, 2)
        return xys

    def disjoint_by_mbr(self, mbr):
        """
        Determine if this Extents shares any points with a minimum bounding
        rectangle.

        :param mbr: Minimum bounding rectangle as defined by TileDB.
        :return: True if no shared points, false otherwise.
        """
        xs, ys = mbr
        x1, x2 = xs
        y1, y2 = ys
        if x1 > self.x2 or x2 < self.x1:
            return True
        if y1 > self.y2 or y2 < self.y1:
            return True
        return False

    def disjoint(self, other):
        """
        Determined if this Extents shares any points with another Extents
        object.

        :param other: Extents object to compare against.
        :return: True if no shared points, false otherwise.
        """
        return self.bounds.disjoint(other.bounds)

    def chunk(
        self,
        data: Data,
        pc_threshold=600000,
    ) -> list[Self]:
        """
        Split up a dataset into tiles based on the point threshold. Unlike Scan
        this will not stop at a specific depth, but keep going until finding
        nodes that fit the point count threshold.

        :param data: Incoming Data object to oeprate on.
        :param pc_threshold: Point count threshold., defaults to 600000
        :return: Return list of Extents that fit the criteria
        """
        data = delayed(data)
        pc_threshold = delayed(pc_threshold)
        tasks = [self.filter(data, pc_threshold)]
        chunks = []
        while tasks:
            batches = list(itertools.batched(tasks, 1000))
            for batch in batches:
                results = compute(batch, scheduler='threads')[0]
                tasks = []
                for r in results:
                    if r is None:
                        continue
                    elif isinstance(r, Extents):
                        chunks.append(r)
                    else:
                        tasks = tasks + r
        return chunks

    @delayed
    def filter(
        self,
        data: Data,
        pc_threshold=600000,
        prev_estimate=0,
    ):
        """
        Creates quad tree of chunks for this bounds, runs pdal quickinfo over
        this to determine if there are any points available. Uses a bottom
        resolution of 1km.

        :param data: Data object containing point cloud details.
        :param res_threshold: Resolution threshold., defaults to 100
        :param pc_threshold: Point count threshold., defaults to 600000
        :param depth_threshold: Tree depth threshold., defaults to 6
        :param depth: Current tree depth., defaults to 0
        :return: Returns a list of Extents.
        """
        pc = data.estimate_count(self.bounds)

        target_pc = pc_threshold
        minx, miny, maxx, maxy = self.bounds.get()

        # is it empty?
        if not pc:
            return None
        else:
            # has it hit the threshold yet?
            next_split_x = (maxx - minx) / 2
            next_split_y = (maxy - miny) / 2

            # if the next split would put our area below the resolution, or if
            # the point count is less than the point threshold then use this
            # tile as the work unit.
            if next_split_x < self.resolution or next_split_y < self.resolution:
                return self
            elif pc <= target_pc:
                return self
            elif pc == prev_estimate:
                return self
            else:
                return [
                    ch.filter(data, pc_threshold, prev_estimate=pc)
                    for ch in self.split()
                ]

    def split(self):
        """
        Split this extent into 4 children along the cell lines

        :return: Returns 4 child extents
        """
        minx, miny, maxx, maxy = self.bounds.get()

        x_adjusted = math.floor((maxx - minx) / 2 / self.resolution)
        y_adjusted = math.ceil((maxy - miny) / 2 / self.resolution)

        midx = minx + (x_adjusted * self.resolution)
        midy = maxy - (y_adjusted * self.resolution)

        return [
            Extents(
                Bounds(minx, miny, midx, midy),
                self.resolution,
                self.alignment,
                self.root,
            ),  # lower left
            Extents(
                Bounds(midx, miny, maxx, midy),
                self.resolution,
                self.alignment,
                self.root,
            ),  # lower right
            Extents(
                Bounds(minx, midy, midx, maxy),
                self.resolution,
                self.alignment,
                self.root,
            ),  # top left
            Extents(
                Bounds(midx, midy, maxx, maxy),
                self.resolution,
                self.alignment,
                self.root,
            ),  # top right
        ]

    def get_leaf_children(self, tile_size):
        """
        Get children Extents with given number of cells per tile.

        :param tile_size: Cells per tile.
        :yield: Yield from list of child extents.
        """
        res = self.resolution
        xnum = math.floor(math.sqrt(tile_size))
        ynum = xnum

        local_xs = np.array(
            [
                [x, min(x + xnum, self.x2)]
                for x in range(self.x1, self.x2, int(xnum))
            ],
            dtype=np.float64,
        )
        dx = (res * local_xs) + self.root.minx

        local_ys = np.array(
            [
                [min(y + ynum, self.y2), y]
                for y in range(self.y1, self.y2, int(ynum))
            ],
            dtype=np.float64,
        )
        dy = self.root.maxy - (res * local_ys)

        coords_list = np.array(
            [[*x, *y] for x in dx for y in dy], dtype=np.float64
        )
        return [
            Extents(
                Bounds(minx, miny, maxx, maxy),
                self.resolution,
                self.alignment,
                self.root,
            )
            for minx, maxx, miny, maxy in coords_list
        ]

    @staticmethod
    def from_storage(storage: str|Storage):
        """
        Create Extents from information stored in database.

        :param storage: SilviMetric Storage object or path to it.
        :return: Returns resulting Extents.
        """
        if isinstance(storage, str):
            storage = Storage.from_db(storage)
        meta = storage.get_config()
        return Extents(meta.root, meta.resolution, meta.alignment, meta.root)

    @staticmethod
    def from_sub(storage: str | Storage, sub: Bounds):
        """
        Create an Extents that is less than the overall extents of the database.

        :param storage: SilviMetric Storage object or path to it.
        :param sub: Desired bounding box.
        :return: Returns resulting Extents.
        """
        if isinstance(storage, str):
            storage = Storage.from_db(storage)

        meta = storage.get_config()
        res = meta.resolution
        align = meta.alignment
        base_extents = Extents(meta.root, res, align, meta.root)
        base = base_extents.bounds

        return Extents(sub, res, align, base)

    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds.get()
        return f'([{minx:.2f},{maxx:.2f}],[{miny:.2f},{maxy:.2f}])'
