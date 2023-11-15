import pdal
import tiledb
import numpy as np
from math import floor

from . import Bounds

class Storage(object):
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self):
        self.ctx = tiledb.default_ctx()

        self.schema = None
        self.tdb = None

    def __make_schema(self, atts: list[str], resolution: float, bounds: list[float]) -> tiledb.ArraySchema:
        """
        Creates TileDB schema for Storage class.

        Parameters
        ----------
        atts : list[str]
            List of PDAL attributes
        resolution : float
            Resolution of a cell
        bounds : list[float]
            Bounding box of dataset. [minx, minz(, minz), maxx, maxy(, maxz)]

        Returns
        -------
        tiledb.ArraySchema
            Schema associated with generated TileDB database
        """

        if len(bounds) == 4:
            minx, miny, maxx, maxy = bounds
        elif len(bounds) == 6:
            minx, miny, minz, maxx, maxy, maxz = bounds
        else:
            raise Exception(f"Bounding box must have either 4 or 6 entities. {bounds}")


        dims = { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in atts }
        xi = floor((maxx - minx) / resolution)
        yi = floor((maxy - miny) / resolution)

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.float64, ctx=self.ctx)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.float64, ctx=self.ctx)
        domain = tiledb.Domain(dim_row, dim_col, ctx=self.ctx)

        count_att = tiledb.Attr(name="count", dtype=np.int32, ctx=self.ctx)
        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True, ctx=self.ctx)
                    for name in atts]

        schema = tiledb.ArraySchema(ctx=self.ctx, domain=domain, sparse=True,
            capacity=len(atts) * xi * yi,
            attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()
        return schema

    def create(self, atts:list[str], resolution: float, bounds: list[float], dirname: str):
        """
        Creates TileDB storage.

        Parameters
        ----------
        atts : list[str]
            List of PDAL attributes
        resolution : float
            Resolution of a cell
        bounds : list[float]
            Bounding box of dataset. [minx, minz(, minz), maxx, maxy(, maxz)]
        dirname : str
            Path to where TileDB should be created
        """
        if tiledb.object_type(dirname) == "array":
            self.tdb = tiledb.open(dirname, "w")
        else:
            schema = self.__make_schema(atts, resolution, bounds)
            tiledb.SparseArray.create(dirname, schema)
            self.tdb = tiledb.open(dirname, "w")

    def write(self, xs: np.ndarray, ys: np.ndarray, data: np.ndarray) -> None:
        """
        Write data to TileDB database

        Parameters
        ----------
        xs : np.ndarray
            X cell indices
        ys : np.ndarray
            Y cell indices
        data : np.ndarray
            Numpy object of data values for attributes in each index pairing
        """
        if not self.tdb:
            self.tdb = tiledb.open(self.dirname, "w")
        self.tdb[xs, ys] = data
