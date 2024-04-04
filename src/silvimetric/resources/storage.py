import tiledb
import numpy as np
from datetime import datetime
import pathlib
import contextlib
import json
from typing import Any

from math import floor

from .config import StorageConfig, ShatterConfig
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
        s.saveConfig()

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
        with self.open('w') as w:
            w.meta['config'] = str(self.config)

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

    def getMetadata(self, key: str, timestamp: int) -> str:
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

        with self.open('r', (timestamp, timestamp)) as r:
            val = r.meta[f'{key}']

        return val


    def saveMetadata(self, key: str, data: str, timestamp: int) -> None:
        """
        Save metadata to storage

        Parameters
        ----------
        key : str
            Metadata key
        data : any
            Metadata value. Must be translatable to and from string.
        """
        with self.open('w', (timestamp, timestamp)) as w:
            w.meta[f'{key}'] = data
        return

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
            if mode in ['w', 'r', 'd', 'm']:
                tdb = tiledb.open(self.config.tdb_dir, mode, timestamp=timestamp)
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

    def reserve_time_slot(self) -> int:
        """
        Increment time slot in database and reserve that spot for a new
        shatter process.

        Parameters
        ----------
        config : ShatterConfig
            Shatter config will be written as metadata to reserve time slot

        Returns
        -------
        int
            Time slot
        """
        with tiledb.open(self.config.tdb_dir, 'r') as r:
            latest = json.loads(r.meta['config'])

        time = latest['next_time_slot']
        latest['next_time_slot'] = time + 1

        with tiledb.open(self.config.tdb_dir, 'w') as w:
            w.meta['config'] = json.dumps(latest)

        return time

    def get_history(self, start_time: datetime, end_time: datetime,
                    bounds: Bounds, name:str=None):
        """
        Retrieve history of the database.

        Parameters
        ----------
        start_time : datetime
        end_time : datetime
        bounds : Bounds
        name : str, optional
            Shatter process uuid, by default None

        Returns
        -------
        tiledb.FragmentInfoList

        """

        af = tiledb.array_fragments(self.config.tdb_dir, True)
        m = [ ]
        for idx in range(len(af)):

            time_range =  af[idx].timestamp_range
            # all shatter processes should be input as a point in time, eg (1,1)
            if isinstance(time_range, tuple):
                time_range = time_range[0]

            try:
                s_str = self.getMetadata('shatter', time_range)
            except KeyError:
                continue
            s = ShatterConfig.from_string(s_str)
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

            m.append(s)

        return m

    def mbrs(self, proc_num):
        timestamp_range = (proc_num, proc_num)

        af_all = tiledb.array_fragments(self.config.tdb_dir, include_mbrs=True)
        mbrs_list = tuple(mbrs for af in af_all for mbrs in af.mbrs
                if af.timestamp_range == timestamp_range)
        mbrs = tuple(tuple(tuple(a.item() for a in mb) for mb in m) for m in mbrs_list)
        return mbrs

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

    def delete(self, proc_num: int) -> None:
        """
        Delete Shatter process and all associated data from database.

        Parameters
        ----------
        proc_num : int
            Shatter process time slot
        """
        with self.open('r', (proc_num, proc_num)) as r:
            sh_cfg = ShatterConfig.from_string(r.meta['shatter'])
            sh_cfg.bounds_done = [ ]
            sh_cfg.finished = False
        with self.open('m', (proc_num, proc_num)) as m:
            m.delete_fragments(proc_num, proc_num)
        with self.open('w', (proc_num, proc_num)) as w:
            w.meta['shatter'] = json.dumps(sh_cfg.to_json())

    def consolidate_shatter(self, proc_num: int) -> None:
        tiledb.consolidate(self.config.tdb_dir, timestamp=(proc_num, proc_num))
        # tiledb.vacuum(self.config.tdb_dir, timestamp=(proc_num, proc_num))