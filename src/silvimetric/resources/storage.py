import pathlib
import contextlib
import json
from struct import Struct
import struct

from math import floor
from datetime import datetime
from typing_extensions import Generator, Optional

import tiledb
from tiledb.metadata import Metadata
import numpy as np
import pandas as pd

from .config import StorageConfig, ShatterConfig
from .metric import Metric, Attribute
from .bounds import Bounds


def ts_overlap(first, second):
    if first[0] > second[1]:
        return False
    if first[1] < second[0]:
        return False
    return True


class Storage:
    """Handles storage of shattered data in a TileDB Database."""

    def __init__(self, config: StorageConfig):
        if not tiledb.object_type(config.tdb_dir) == 'array':
            raise Exception(
                f"Given database directory '{config.tdb_dir}' does not exist"
            )

        self.config = config
        self.reader: tiledb.SparseArray = None
        self.writer: tiledb.SparseArray = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return

    @staticmethod
    def create(config: StorageConfig, ctx: tiledb.Ctx = None):
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

        if ctx is None:
            ctx = tiledb.default_ctx()

        # adjust cell bounds if necessary
        config.root.adjust_alignment(config.resolution, config.alignment)

        xi = floor(
            (config.root.maxx - config.root.minx) / float(config.resolution)
        )
        yi = floor(
            (config.root.maxy - config.root.miny) / float(config.resolution)
        )

        dim_row = tiledb.Dim(
            name='X',
            domain=(0, xi),
            dtype=np.int32,
            filters=tiledb.FilterList([ tiledb.ZstdFilter() ]),
        )
        dim_col = tiledb.Dim(
            name='Y',
            domain=(0, yi),
            dtype=np.int32,
            filters=tiledb.FilterList([ tiledb.ZstdFilter() ]),
        )
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(
            name='count',
            dtype=np.int32,
            filters=tiledb.FilterList([ tiledb.ZstdFilter() ]),
        )
        proc_att = tiledb.Attr(
            name='shatter_process_num',
            dtype=np.uint64,
            filters=tiledb.FilterList([ tiledb.ZstdFilter() ]),
        )
        dim_atts = [attr.schema() for attr in config.attrs]

        metric_atts = [
            m.schema(a)
            for m in config.metrics
            for a in config.attrs
            if a in m.attributes or not m.attributes
        ]

        # Check that all attributes required for metric usage are available
        att_list = [a.name for a in config.attrs]
        required_atts = [
            d.name
            for m in config.metrics
            for d in m.dependencies
            if isinstance(d, Attribute)
        ]
        for ra in required_atts:
            if ra not in att_list:
                raise ValueError(f'Missing required dependency, {ra}.')

        # allows_duplicates lets us insert multiple values into each cell,
        # with each value representing a set of values from a shatter process
        # https://docs.tiledb.com/main/how-to/performance/performance-tips/summary-of-factors#allows-duplicates
        schema = tiledb.ArraySchema(
            domain=domain,
            attrs=[
                count_att,
                proc_att,
                *dim_atts,
                *metric_atts,
            ],
            allows_duplicates=True,
            sparse=True,
            offsets_filters=tiledb.FilterList(
                [
                    tiledb.PositiveDeltaFilter(),
                    tiledb.BitWidthReductionFilter(),
                    tiledb.ZstdFilter(),
                ]
            ),
        )
        schema.check()

        tiledb.SparseArray.create(config.tdb_dir, schema)
        writer = tiledb.SparseArray(config.tdb_dir, 'w')
        writer.meta['config'] = str(config)

        s = Storage(config)
        s.writer = writer
        s.save_config()

        return s

    @staticmethod
    def from_db(tdb_dir: str, ctx: tiledb.Ctx = None):
        """
        Create Storage object from information stored in a database.

        :param tdb_dir: TileDB database directory.
        :return: Returns the derived storage.
        """
        if ctx is None:
            ctx = tiledb.default_ctx()

        reader = tiledb.open(tdb_dir, 'r')
        metadata = reader.meta
        s = metadata['config']
        config = StorageConfig.from_string(s)

        # in case a database has been copied somewhere else
        config.tdb_dir = tdb_dir
        storage = Storage(config)

        # set the metadata for storage object so we don't have to query again
        storage._meta = metadata
        storage.reader = reader
        storage.save_config()

        return storage

    def save_config(self) -> None:
        """
        Save StorageConfig to the Database
        """
        # build metadata, we'll only requery it if we can't find the desired
        # key later
        with self.open('w') as w:
            w.meta['config'] = str(self.config)
        with self.open('r') as r:
            self._meta = r.meta

    def get_config(self) -> StorageConfig:
        """
        Get the StorageConfig currently in use by Storage.

        :return: StorageConfig representing this object.
        """
        meta_str = self.get_metadata('config')
        return StorageConfig.from_string(meta_str)

    def save_shatter_meta(self, config: ShatterConfig):
        """
        Save shatter metadata to the base TileDB metadata with the name
        convention `shatter_{proc_num}`
        """
        key = f'shatter_{config.time_slot}'
        data = json.dumps(config.to_json())
        return self.save_metadata(key, data)

    def get_shatter_meta(self, time_slot: int):
        """
        Get shatter metadata from the base TileDB metadata with the name
        convention `shatter_{proc_num}`
        """
        key = f'shatter_{time_slot}'
        m = self.get_metadata(key)
        return ShatterConfig.from_string(m)

    def get_metadata(self, key: str) -> str:
        """
        Return metadata at given key. Check first for this key in the
        _meta member variable. If it's not there, we'll check the metadata
        in the db.

        :param key: Key to look for in metadata.
        :return: Metadata value found in storage.
        """
        # if meta hasn't been set up, do so
        if self._meta is None:
            with self.open('r') as tdb:
                self._meta = tdb.meta
                # this should be latest metadata, handle a keyerror higher up
                return self._meta[key]

        # if it was already set up and the key doesn't exist,
        # reload metadata and check there
        if key not in self._meta.keys():
            with self.open('r') as tdb:
                self._meta = tdb.meta

        return self._meta['key']


    def save_metadata(self, key: str, data: str) -> None:
        """
        Save metadata to storage.

        :param key: Metadata key to save to.
        :param data: Data to save to metadata.
        """
        # if writer isn't set up, do it now
        if self._writer is None:
            self._writer = self.open('w')

        # propogate the key-value to both tiledb and the local copy
        self._meta[key] = data
        self._writer.meta[key] = data

    def get_tdb_context(self):
        cfg = tiledb.Config()
        cfg['vfs.s3.connect_scale_factor'] = '25'
        cfg['vfs.s3.connect_max_retries'] = '10'
        # cfg['vfs.s3.max_parallel_ops'] = '1'
        ctx = tiledb.Ctx(cfg)
        return ctx

    def get_attributes(self) -> list[Attribute]:
        """
        Find list of attribute names from storage config.

        :return: List of attribute names.
        """
        return self.config.attrs

    def get_metrics(self) -> list[Metric]:
        """
        Find List of metric names from storage config

        :return: List of metric names.
        """
        return self.config.metrics

    def get_derived_names(self) -> list[str]:
        # if no attributes are set in the metric, use all
        return [
            m.entry_name(a.name)
            for m in self.config.metrics
            for a in self.config.attrs
            if not m.attributes or a.name in [ma.name for ma in m.attributes]
        ]

    def open(
        self, mode: str = 'r', timestamp=None
    ) -> Generator[tiledb.SparseArray, None, None]:
        """
        Open stream for TileDB database in given mode and at given timestamp.

        :param mode: Mode to open TileDB stream in. Valid options are
            'w', 'r', 'm', 'd'., defaults to 'r'.
        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :raises Exception: Incorrect Mode, only valid modes are 'w' and 'r'.
        :raises Exception: Path exists and is not a TileDB array.
        :raises Exception: Path does not exist.
        :yield: TileDB array context manager.
        """

        # tiledb and dask have bad interaction with opening an array if
        # other threads present
        ctx = self.get_tdb_context()

        if tiledb.object_type(self.config.tdb_dir) == 'array':
            if mode in ['w', 'r', 'd', 'm']:
                tdb = tiledb.open(
                    self.config.tdb_dir, mode, timestamp=timestamp, ctx=ctx
                )
            else:
                raise Exception(f"Given open mode '{mode}' is not valid")
        elif pathlib.Path(self.config.tdb_dir).exists():
            raise Exception(
                f'Path {self.config.tdb_dir} already exists and is not'
                ' initialized for TileDB access.'
            )
        else:
            raise Exception(f'Path {self.config.tdb_dir} does not exist')

        return tdb

    def write(self, data_in: pd.DataFrame, timestamp):
        data_in = data_in.rename(columns={'xi': 'X', 'yi': 'Y'})

        attr_dict = {f'{a.name}': a.dtype for a in self.config.attrs}
        xy_dict = {'X': data_in.X.dtype, 'Y': data_in.Y.dtype}
        metr_dict = {
            f'{m.entry_name(a.name)}': np.dtype(m.dtype)
            for m in self.config.metrics
            for a in self.config.attrs
        }
        dtype_dict = attr_dict | xy_dict | metr_dict

        varlen_types = {a.dtype for a in self.config.attrs}

        fillna_dict = {
            f'{m.entry_name(a.name)}': m.nan_value
            for m in self.config.metrics
            for a in self.config.attrs
        }

        ctx = self.get_tdb_context()

        return tiledb.from_pandas(
            uri=self.config.tdb_dir,
            ctx=ctx,
            sparse=True,
            dataframe=data_in,
            mode='append',
            timestamp=timestamp,
            column_types=dtype_dict,
            varlen_types=varlen_types,
            fillna=fillna_dict,
        )

    def reserve_time_slot(self) -> int:
        """
        Increment time slot in database and reserve that spot for a new
        shatter process.

        :param config: Shatter config will be written as metadata to reserve
            time slot.

        :return: Time slot.
        """
        latest = self.get_metadata('config')

        time = latest['next_time_slot']
        latest['next_time_slot'] = time + 1
        self.save_metadata('config', json.dumps(latest))

        self.config.next_time_slot = latest['next_time_slot']
        return time

    def get_history(
        self,
        start_time: datetime,
        end_time: datetime,
        bounds: Bounds,
        name: Optional[str] = None,
        concise: bool = False,
    ):
        """
        Retrieve history of the database at current point in time.

        :param start_time: Query parameter, starting datetime of process.
        :param end_time: Query parameter, ending datetime of process.
        :param bounds: Query parameter, bounds to query by.
        :param name: Query paramter, shatter process uuid., by default None
        :param concise: Whether or not to give shortened version of history.
        :return: Returns list of array fragments that meet query parameters.
        """

        m = []
        for idx in range(1, self.config.next_time_slot):
            s = self.get_shatter_meta(idx)
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

            if concise:
                h = s.history_json()
            else:
                h = s.to_json()
            m.append(h)

        return m

    def mbrs(self, timestamp):
        """
        Get minimum bounding rectangle of a given shatter process. If this
        process has been finished and consolidated the mbr will be much less
        granulated than if the fragments are still intact. Mbrs are represented
        as tuples in the form of ((minx, maxx), (miny, maxy))

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        """
        af_all = self.get_fragments_by_time(timestamp)
        mbrs_list = tuple(mbrs for af in af_all for mbrs in af.mbrs)
        mbrs = tuple(
            tuple(tuple(a.item() for a in mb) for mb in m) for m in mbrs_list
        )
        return mbrs

    def get_fragments_by_time(self, timestamp) -> list[tiledb.FragmentInfo]:
        """
        Get TileDB array fragments from the time slot specified.

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :return: Array fragments from time slot.
        """

        af = tiledb.array_fragments(self.config.tdb_dir, include_mbrs=True)
        return [a for a in af if ts_overlap(a.timestamp_range, timestamp)]

    def delete(self, time_slot: int) -> ShatterConfig:
        """
        Delete Shatter process and all associated data from database.

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :return: Config of deleted Shatter process
        """

        self.config.log.debug(f'Deleting time slot {time_slot}...')
        with self.open('r') as r:
            sh_cfg = ShatterConfig.from_string(r.meta[f'shatter_{time_slot}'])
            sh_cfg.mbr = ()
            sh_cfg.finished = False

        self.config.log.debug('Deleting fragments...')
        with self.open('d') as d:
            d.query(cond=f'shatter_process_num=={time_slot}').submit()

        self.config.log.debug('Rewriting config.')
        with self.open('w') as w:
            w.meta[f'shatter_{time_slot}'] = json.dumps(sh_cfg.to_json())

        return sh_cfg

    def consolidate_shatter(self, timestamp, retries=0) -> None:
        """
        Consolidate the fragments from a shatter process into one fragment.
        This makes the database perform better, but reduces the granularity of
        time traveling.
        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        """
        self.config.log.info('Consolidating db.')
        try:
            tiledb.consolidate(self.config.tdb_dir, timestamp=timestamp)
            c = tiledb.Config({'sm.vacuum.mode': 'fragments'})
            tiledb.vacuum(self.config.tdb_dir, c)
            self.config.log.info(f'Consolidated time slot {timestamp}.')
        except Exception as e:
            if retries >= 3:
                self.config.log.warning(
                    'Failed to consolidate time slot '
                    f'{timestamp} {retries} time(s). Stopping.'
                )
                raise e
            self.config.log.warning(
                'Failed to consolidate time slot '
                f'{timestamp} {retries + 1} time. Retrying...'
            )
            self.consolidate_shatter(timestamp, retries + 1)
