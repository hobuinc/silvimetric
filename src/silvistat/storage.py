import pdal
import tiledb
import numpy as np
from math import floor
import pathlib

from . import Bounds

class Storage(object):
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self, tdb_dir: str, ctx:tiledb.Ctx=None):
        if not ctx:
            self.ctx = tiledb.default_ctx()
        else:
            self.ctx = ctx

        #TODO check paths with pathlib
        self.tdb_dir = tdb_dir

        #TODO create boths streams at startup
        self.__open()
        self.write_flag = False

        self.schema = self.tdb_r.schema

    #TODO enter and exit methods to close read and write streams

    def __enter__(self):
        return zip(self.tdb_r, self.tdb_w)

    def __exit__(self):
        self.tdb_r.close()
        self.tdb_w.close()

    @staticmethod
    def create(atts:list[str], resolution: float, bounds: list[float],
               dirname: str, ctx:tiledb.Ctx=None):
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
        ctx : tiledb.Ctx, optional
            TileDB Context, by default is None

        Returns
        -------
        Storage
            Returns newly created Storage class

        Raises
        ------
        Exception
            Raises bounding box errors if not of lengths 4 or 6
        """

        #TODO pathlib.path for dirname
        if not ctx:
            ctx = tiledb.default_ctx()

        if len(bounds) == 4:
            minx, miny, maxx, maxy = bounds
        elif len(bounds) == 6:
            minx, miny, minz, maxx, maxy, maxz = bounds
        else:
            raise Exception(f"Bounding box must have either 4 or 6 entities. {bounds}")


        dims = { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in atts }
        xi = floor((maxx - minx) / float(resolution))
        yi = floor((maxy - miny) / float(resolution))

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.float64, ctx=ctx)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.float64, ctx=ctx)
        domain = tiledb.Domain(dim_row, dim_col, ctx=ctx)

        count_att = tiledb.Attr(name="count", dtype=np.int32, ctx=ctx)
        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True, ctx=ctx)
                    for name in atts]

        schema = tiledb.ArraySchema(ctx=ctx, domain=domain, sparse=True,
            capacity=len(atts) * xi * yi,
            attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()

        tiledb.SparseArray.create(dirname, schema)

        return Storage(dirname, ctx)

    def __open(self):
        p = pathlib.Path(self.tdb_dir)
        if tiledb.object_type(self.tdb_dir) == "array":
            self.tdb_w: tiledb.SparseArray = tiledb.SparseArray(self.tdb_dir, "w", ctx=self.ctx)
            self.tdb_r: tiledb.SparseArray = tiledb.SparseArray(self.tdb_dir, "r", ctx=self.ctx)
        elif p.exists():
            raise Exception(f"""Path {self.tdb_dir} already exists and is not initialized\
                             for TileDB access.""")
        else:
            raise Exception(f"Path {self.tdb_dir} does not exist")

    #TODO what are we reading? queries are probably going to be specific
    def read(self, xs: np.ndarray, ys: np.ndarray) -> np.ndarray:
        """
        Read from the Database
        Parameters
        ----------
        xs : np.ndarray
            X index
        ys : np.ndarray
            Y index

        Returns
        -------
        np.ndarray
            Items found at the indicated cell
        """
        if self.write_flag:
            self.tdb_r.reopen()
        return self.tdb_r[xs, ys]

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
        self.tdb_w[xs, ys] = data
        self.write_flag = True
