import pdal
import tiledb
import numpy as np
from math import floor
import pathlib

from .config import Configuration
from .metric import Metrics

class Storage:
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self, config: Configuration, ctx:tiledb.Ctx=None):
        # if not ctx:
        #     self.ctx = tiledb.default_ctx()
        # else:
        #     self.ctx = ctx

        if not pathlib.Path(config.tdb_dir).exists():
            raise Exception(f"Given database directory '{config.tdb_dir}' does not exist")

        self.config = config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return


    @staticmethod
    def create( config:Configuration, ctx:tiledb.Ctx=None):
        """
        Creates TileDB storage.

        Parameters
        ----------
        config : Configuration
            Storage configuration
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

        dims = { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in config.attrs }
        xi = floor((config.bounds.maxx - config.bounds.minx) / float(config.resolution))
        yi = floor((config.bounds.maxy - config.bounds.miny) / float(config.resolution))

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.int32)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.int32)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        dim_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True)
                    for name in config.attrs]

        metric_atts = [
            tiledb.Attr(
                name=f'm_{att}_{m}',
                dtype=Metrics[m].dtype
            )
            for m in config.metrics
            for att in config.attrs
        ]

        # allows_duplicates lets us insert multiple values into each cell,
        # with each value representing a set of values from a shatter process
        # https://docs.tiledb.com/main/how-to/performance/performance-tips/summary-of-factors#allows-duplicates
        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=16, attrs=[count_att, *dim_atts, *metric_atts],
            allows_duplicates=True)
        schema.check()

        tiledb.SparseArray.create(config.tdb_dir, schema)
        with tiledb.SparseArray(config.tdb_dir, "w") as a:
            a.meta['config'] = str(config)

        s = Storage(config, ctx)

        return s

    @staticmethod
    def from_db(tdb_dir: str):
        """
        Create Storage class from previously created storage path

        Parameters
        ----------
        tdb_dir : str
            Path to storage directory

        Returns
        -------
        Storage
            Return the generated storage
        """
        with tiledb.open(tdb_dir, 'r') as a:
            s = a.meta['config']
            config = Configuration.from_string(s)
            return Storage(config)

    def saveConfig(self) -> None:
        """
        Save configuration to the Database

        """
        # reopen in write mode if current mode is read
        with self.open('w') as a:
            a.meta['config'] = str(self.config)

    def getConfig(self) -> Configuration:
        """
        Get the Configuration currently in use by the Storage

        Returns
        -------
        Configuration
            Configuration object
        """
        # reopen in read mode if current mode is write
        with self.open('r') as a:
            s = a.meta['config']
            config = Configuration.from_string(s)
            return config

    def getMetadata(self, key: str) -> str:
        """
        Return metadata at given key

        Parameters
        ----------
        key : str
            Metadata key

        Returns
        -------
        str
            Metadata value found in storage
        """
        with self.open('r') as r:
            try:
                val = r.meta[key]
                return val
            except KeyError:
                return None

    def getAttributes(self) -> list[str]:
        """
        Return list of attribute names from storage config

        Returns
        -------
        list[str]
            List of attribute names
        """
        return self.getConfig().attrs

    def getMetrics(self) -> list[str]:
        """
        Return List of metric names from storage config

        Returns
        -------
        list[str]
            List of metric names
        """
        return self.getConfig().metrics

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

        if tiledb.object_type(self.config.tdb_dir) == "array":
            if mode == 'w':
                tdb: tiledb.SparseArray = tiledb.open(self.config.tdb_dir, 'w')
            elif mode == 'r':
                tdb: tiledb.SparseArray = tiledb.open(self.config.tdb_dir, 'r')
            else:
                raise Exception(f"Given open mode '{mode}' is not valid")
        elif pathlib.Path(self.config.tdb_dir).exists():
            raise Exception(f"Path {self.config.tdb_dir} already exists and is not" +
                            " initialized for TileDB access.")
        else:
            raise Exception(f"Path {self.config.tdb_dir} does not exist")
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