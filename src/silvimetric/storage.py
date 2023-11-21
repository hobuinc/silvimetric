import pdal
import tiledb
import numpy as np
from math import floor
import pathlib
import pyproj
from time import sleep

import asyncio
from redis import Redis
from pottery import Redlock

class Storage(object):
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self, tdb_dir: str, ctx:tiledb.Ctx=None, redis_url:str=None):
        # if not ctx:
        #     self.ctx = tiledb.default_ctx()
        # else:
        #     self.ctx = ctx

        if not pathlib.Path(tdb_dir).exists():
            raise Exception(f"Given database directory '{tdb_dir}' does not exist")

        self.tdb_dir: str = tdb_dir
        self.redis = Redis.from_url(redis_url) if redis_url is not None else None


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

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True, fill=0)
                    for name in atts]

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=len(atts) * xi * yi * 10000,
            attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()

        try:
            proj_crs = pyproj.CRS.from_user_input(crs)
        except:
            raise Exception(f"Invalid CRS ingested, {crs}")

        tiledb.SparseArray.create(dirname, schema)
        with tiledb.SparseArray(dirname, "w") as A:
            metadata = {'resolution': resolution}
            metadata['bounds'] = [minx, miny, maxx, maxy]
            metadata['crs'] = proj_crs.to_string()
            A.meta.update(metadata)

        s = Storage(dirname)

        return s

    def consolidate(self, ctx=None):
        # if not ctx:
        #     ctx = self.ctx
        tiledb.consolidate(self.tdb_dir)

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
                tdb: tiledb.SparseArray = tiledb.open(self.tdb_dir, 'w')
            elif mode == 'r':
                tdb: tiledb.SparseArray = tiledb.open(self.tdb_dir, 'r')
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
        ## if using redis, acquire a lock key, otherwise, acquire mutex
        # lock_list = [
        #     Redlock(key=f'{x}_{y}_lock', masters={self.redis}, auto_release_time=10)
        #     for x,y in zip(xs,ys)
        # ]

        with self.open('w') as tdb:
            if self.redis is not None:
                for x,y,d in zip(xs, ys, data):
                    lock = Redlock(key=f'{x}_{y}_lock', masters={self.redis}, auto_release_time=0.2)
                    with lock:
                        with self.open('r') as tdb_r:
                            prev = tdb_r[x,y]
                            if bool(np.any(prev)):
                                for att in prev:
                                    d[att] = np.concatenate((d[att], prev[att]))
                        tdb[x, y] = d