import tiledb
import numpy as np
from datetime import datetime
import pathlib
import contextlib
import json
from typing import Any

from math import floor

from .config import StorageConfig
from .metric import Metric, Attribute
from ..resources import Bounds

class Storage:
    """ Handles storage of shattered data in a TileDB Database. """

    def __init__(self, config: StorageConfig, ctx:tiledb.Ctx=None):
        # if not ctx:
        #     self.ctx = tiledb.default_ctx()
        # else:
        #     self.ctx = ctx

        if not tiledb.object_type(config.tdb_dir) == "array":
            raise Exception(f"Given database directory '{config.tdb_dir}' does not exist")

        self.config = config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return


    @staticmethod
    def create( config:StorageConfig, ctx:tiledb.Ctx=None):
        """
        Creates TileDB storage.

        Parameters
        ----------
        config : StorageConfig
            Storage StorageConfig
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

        # TODO make any changes to tiledb setup here.

        if not ctx:
            ctx = tiledb.default_ctx()

        # dims = { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in config.attrs }
        xi = floor((config.root.maxx - config.root.minx) / float(config.resolution))
        yi = floor((config.root.maxy - config.root.miny) / float(config.resolution))

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.int32)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.int32)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)
        dim_atts = [attr.schema() for attr in config.attrs]

        metric_atts = [m.schema(a) for m in config.metrics for a in config.attrs]

        # allows_duplicates lets us insert multiple values into each cell,
        # with each value representing a set of values from a shatter process
        # https://docs.tiledb.com/main/how-to/performance/performance-tips/summary-of-factors#allows-duplicates
        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            attrs=[count_att, *dim_atts, *metric_atts], allows_duplicates=True)
        schema.check()

        tiledb.SparseArray.create(config.tdb_dir, schema)
        with tiledb.SparseArray(config.tdb_dir, "w") as a:
            meta = str(config)
            a.meta['config'] = meta

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
            config = StorageConfig.from_string(s)

        return Storage(config)

    def saveConfig(self) -> None:
        """
        Save StorageConfig to the Database

        """
        with self.open('w') as a:
            a.meta['config'] = str(self.config)

    def getConfig(self) -> StorageConfig:
        """
        Get the StorageConfig currently in use by the Storage

        Returns
        -------
        StorageConfig
            StorageConfig object
        """
        with self.open('r') as a:
            s = a.meta['config']
            config = StorageConfig.from_string(s)

        return config

    def getMetadata(self, key: str, default=None) -> str:
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
            except KeyError as e:
                if default is not None:
                    return default
                raise(e)

        return val

    def saveMetadata(self, key: str, data: Any) -> None:
        """
        Save metadata to storage

        Parameters
        ----------
        key : str
            Metadata key
        data : any
            Metadata value. Must be translatable to and from string.
        """
        with self.open('w') as w:
            w.meta[key] = data

    def getAttributes(self) -> list[Attribute]:
        """
        Return list of attribute names from storage config

        Returns
        -------
        list[str]
            List of attribute names
        """
        return self.getConfig().attrs

    def getMetrics(self) -> list[Metric]:
        """
        Return List of metric names from storage config

        Returns
        -------
        list[str]
            List of metric names
        """
        return self.getConfig().metrics

    @contextlib.contextmanager
    def open(self, mode:str='r', timestamp=None) -> tiledb.SparseArray:
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
                tdb = tiledb.open(self.config.tdb_dir, 'w', timestamp=timestamp)
            elif mode == 'r':
                tdb = tiledb.open(self.config.tdb_dir, 'r', timestamp=timestamp)
            else:
                raise Exception(f"Given open mode '{mode}' is not valid")
        elif pathlib.Path(self.config.tdb_dir).exists():
            raise Exception(f"Path {self.config.tdb_dir} already exists and is not" +
                            " initialized for TileDB access.")
        else:
            raise Exception(f"Path {self.config.tdb_dir} does not exist")

        try:
            yield tdb
        finally:
            tdb.close()

    def get_history(self, start_time: datetime, end_time: datetime,
                    bounds: Bounds, name:str=None):

        from ..resources import ShatterConfig

        af = tiledb.array_fragments(self.config.tdb_dir, True)
        m = [ ]
        for idx in range(len(af)):

            # if processes are too short, begin and end times can be the same
            # so it's necessary to adjust time bounds so we can grab information

            if idx == 0:
                begin = 0
            else:
                begin = af[idx-1].timestamp_range[1]

            end = af[idx].timestamp_range[1]

            with self.open('r', (begin, end)) as r:
                try:
                    if not bool(r.meta['shatter']):
                        continue

                    s = ShatterConfig.from_string(r.meta['shatter'])

                    # filter bounds
                    if s.bounds.disjoint(bounds):
                        continue

                    # filter name
                    if name is not None and name != s.name:
                        continue

                    # filter dates
                    if isinstance(s.date, tuple) and len(s.date) == 2:
                        if s.date[1] < start_time or s.date[0] > end_time:
                            continue
                    elif isinstance(s.date, tuple) and len(s.date) == 1:
                        if s.date[0] < start_time or s.date[0] > end_time:
                            continue
                    else:
                        if s.date < start_time or s.date > end_time:
                            continue

                    m.append(json.loads(r.meta['shatter']))
                except KeyError:
                    continue

        return m

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
            # data = {k: v.astype(np.dtype(v.dtype)) for k,v in data.items()}

            # if self.config.app.debug:
            #     tiledb.stats_reset()
            #     tiledb.stats_enable()
            tdb[xs, ys] = data

            # if self.config.app.debug:
            #     tiledb.stats_dump()
            #     tiledb.stats_reset()
