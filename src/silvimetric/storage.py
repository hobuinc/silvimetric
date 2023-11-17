import pdal
import tiledb
import numpy as np
from math import floor
import pathlib
import pyproj

class Storage(object):
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self, tdb_dir: str, ctx:tiledb.Ctx=None):
        if not ctx:
            self.ctx = tiledb.default_ctx()
        else:
            self.ctx = ctx

        if not pathlib.Path(tdb_dir).exists():
            raise Exception(f"Given database directory '{tdb_dir}' does not exist")

        self.tdb_dir: str = tdb_dir


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return


    @staticmethod
    def create(atts:list[str], resolution: float, bounds: list[float],
               dirname: str, crs: str, ctx:tiledb.Ctx=None):
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

        try:
            proj_crs = pyproj.CRS.from_user_input(crs)
        except:
            raise Exception(f"Invalid CRS ingested, {crs}")

        tiledb.SparseArray.create(dirname, schema)
        with tiledb.SparseArray(dirname, "w", ctx=ctx) as A:
            metadata = {'resolution': resolution}
            metadata['bounds'] = [minx, miny, maxx, maxy]
            metadata['crs'] = proj_crs.to_string()
            A.meta.update(metadata)

        s = Storage(dirname, ctx=ctx)

        return s

    def saveMetadata(self, metadata: dict) -> None:
        """
        Save metadata to the Database

        Parameters
        ----------
        metadata : dict
            Metadata key-value pairs to be saved
        """
        # reopen in write mode if current mode is read
        with self.open('w') as a:
            a.meta.update(metadata)

    def getMetadata(self) -> dict:
        """
        Get the metadata from the database

        Returns
        -------
        dict
            Dictionary of key-value pairs of database metadata
        """
        # reopen in read mode if current mode is write
        with self.open('r') as a:
            data = dict(a.meta)
        return data

    def getAttributes(self) -> list[str]:
        with self.open('r') as a:
            s = a.schema
            att_list = []
            for idx in range(s.nattr):
                att_list.append(s.attr(idx).name)
        return att_list

    def open(self, mode:str='r') -> tiledb.SparseArray:
        """
        Open either a read or write stream for TileDB database

        Parameters
        ----------
        mode : str, optional
            Stream mode. Valid options are 'r' and 'w', by default 'r'

        Raises
        ------
        Exception
            Incorrect Mode was given, only valid modes are 'w' and 'r'
        Exception
            Path exists and is not a TileDB array
        Exception
            Path does not exist
        """

        if tiledb.object_type(self.tdb_dir) == "array":
            if mode == 'w':
                tdb: tiledb.SparseArray = tiledb.SparseArray(self.tdb_dir, "w", ctx=self.ctx)
            elif mode == 'r':
                tdb: tiledb.SparseArray = tiledb.SparseArray(self.tdb_dir, "r", ctx=self.ctx)
            else:
                raise Exception(f"Given open mode '{mode}' is not valid")
        elif pathlib.Path(self.tdb_dir).exists():
            raise Exception(f"Path {self.tdb_dir} already exists and is not" +
                            " initialized for TileDB access.")
        else:
            raise Exception(f"Path {self.tdb_dir} does not exist")
        return tdb

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
        with self.open('r') as tdb:
            data = tdb[xs, ys]
        return data

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
        with self.open('w') as tdb:
            tdb[xs, ys] = data
