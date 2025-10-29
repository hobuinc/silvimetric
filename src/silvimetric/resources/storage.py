import json

import os
from urllib.parse import urlparse
from pathlib import Path
from math import floor
from typing_extensions import Optional, Union, Literal
from datetime import datetime

import tiledb
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

        self.config: StorageConfig = config
        self._reader: tiledb.DenseArray = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self._reader is not None:
            self._reader.close()
        self._reader = None
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
            dtype=np.uint64,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
        )
        dim_col = tiledb.Dim(
            name='Y',
            domain=(0, yi),
            dtype=np.uint64,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
        )
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(
            name='count',
            dtype=np.uint32,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
            fill=0,
        )
        proc_att = tiledb.Attr(
            name='shatter_process_num',
            dtype=np.uint16,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
            fill=0,
        )
        start_time_att = tiledb.Attr(
            name='start_time',
            dtype=np.datetime64('', 'D').dtype,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
            fill=np.datetime64(0, 'D')
        )
        end_time_att = tiledb.Attr(
            name='end_time',
            dtype=np.datetime64('', 'D').dtype,
            filters=tiledb.FilterList([tiledb.ZstdFilter()]),
            fill=np.datetime64(0, 'D')
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
                start_time_att,
                end_time_att,
                *dim_atts,
                *metric_atts,
            ],
            offsets_filters=tiledb.FilterList(
                [
                    tiledb.PositiveDeltaFilter(),
                    tiledb.BitWidthReductionFilter(),
                    tiledb.ZstdFilter(),
                ]
            ),
        )
        schema.check()

        tiledb.DenseArray.create(config.tdb_dir, schema)
        with tiledb.DenseArray(config.tdb_dir, 'w') as writer:
            writer.meta['config'] = str(config)

        s = Storage(config)
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
        storage._reader = reader
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
        if self._reader is not None:
            self._reader.reopen()

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
        reader = self.open('r')
        try:
            return reader.meta[key]
        except KeyError:
            reader.reopen()
            self._reader = reader
            return reader.meta[key]

    def save_metadata(self, key: str, data: str) -> None:
        """
        Save metadata to storage.

        :param key: Metadata key to save to.
        :param data: Data to save to metadata.
        """
        # if writer isn't set up, do it now
        # propogate the key-value to both tiledb and the local copy
        with self.open('w') as w:
            w.meta[key] = data
        if self._reader is not None:
            self._reader.reopen()

    def get_tdb_context(self):
        cfg = tiledb.Config()
        cfg['vfs.s3.connect_scale_factor'] = '25'
        cfg['vfs.s3.connect_max_retries'] = '10'
        ctx = tiledb.Ctx(cfg)
        return ctx

    def get_attributes(
        self, names: Optional[list[str]] = None
    ) -> list[Attribute]:
        """
        Find list of attribute names from storage config.

        :param names: List of Metric names to get.
        :return: List of attribute names.
        """
        if names is not None:
            return [a for a in self.config.attrs if a.name in names]

        return self.config.attrs

    def get_metrics(self, names: Optional[list[str]] = None) -> list[Metric]:
        """
        Find List of metric names from storage config

        :param names: List of Metric names to get.
        :return: List of metric names.
        """
        if names is not None:
            return [m for m in self.config.metrics if m.name in names]
        return self.config.metrics

    def get_derived_names(
        self,
        metrics: Optional[list[str, Metric]] = None,
        attributes: Optional[list[str, Attribute]] = None,
    ) -> list[str]:
        """
        Return names of TileDB Attribute names based on combination of
        Metrics and SilviMetric Attributes. If none are specified, grab
        all Metrics and Attributes from Storage Config.
        """
        if metrics is None:
            metrics = self.config.metrics
        if attributes is None:
            attributes = self.config.attrs

        # if no attributes are set in the metric, use all
        return [
            m.entry_name(a.name)
            for m in metrics
            for a in attributes
            if not m.attributes or a.name in [ma.name for ma in m.attributes]
        ]

    def open(self, mode: str = 'r', timestamp=None) -> tiledb.SparseArray:
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

        # non-timestamped reader and writer are stored as member variables to
        # avoid opening and closing too many io objects.
        if timestamp is not None or mode != 'r':
            return tiledb.open(
                self.config.tdb_dir, mode, timestamp=timestamp, ctx=ctx
            )
        else:  # no timestamp and mode is 'r'
            if self._reader is None or not self._reader.isopen:
                self._reader = tiledb.open(self.config.tdb_dir, 'r')

            self._reader.reopen()
            return self._reader

    def write(self, data_in: pd.DataFrame, dates: tuple[datetime, datetime]):
        """Write to TileDB Array."""

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

        # so tiledb knows how to fill null spots
        fillna_dict = {
            f'{m.entry_name(a.name)}': m.nan_value
            for m in self.config.metrics
            for a in self.config.attrs
        }
        fillna_dict['count'] = 0
        fillna_dict['shatter_process_num'] = 0

        # TileDB can't handle null cell writes for variable length arrays, so
        # make sure that any index in the dense block that doesn't have a value
        # is fill with a designated null value
        xi_vals = data_in.X
        yi_vals = data_in.Y
        xrange = range(xi_vals.min(), xi_vals.max() + 1)
        yrange = range(yi_vals.min(), yi_vals.max() + 1)
        mi = pd.MultiIndex.from_product([xrange, yrange], names=['X', 'Y'])
        d = data_in.set_index(['X', 'Y'])

        listed = d.reindex(mi)
        isna = listed[self.config.attrs[0].name].isna()
        if isna.any():
            listed = listed.fillna(fillna_dict)
            for attr, attr_type in attr_dict.items():
                dtype = attr_type.subtype
                kind = np.dtype(dtype).kind
                if kind in ['i', 'f']:
                    nan_value = -9999
                elif kind == 'u':
                    nan_value = 0
                else:
                    nan_value = -9999
                listed.loc[isna, attr] = pd.Series(
                    [np.array([nan_value], dtype=dtype)] * isna.sum()
                ).values
            data_in = listed.reset_index()

        # timestamp call does seconds since epoch, tiledb requires nanoseconds
        data_in = data_in.assign(
            start_time=np.datetime64(dates[0], 'ns')
        ).assign(end_time=np.datetime64(dates[1], 'ns'))

        ctx = self.get_tdb_context()
        tiledb.from_pandas(
            uri=self.config.tdb_dir,
            ctx=ctx,
            sparse=False,
            dataframe=data_in,
            mode='append',
            column_types=dtype_dict,
            varlen_types=varlen_types,
            fillna=fillna_dict,
            fit_to_df=True,
        )

    def reserve_time_slot(self) -> int:
        """
        Increment time slot in database and reserve that spot for a new
        shatter process.

        :param config: Shatter config will be written as metadata to reserve
        time slot.

        :return: Time slot.
        """
        # make sure we're dealing with the latest config
        cfg = self.get_config()
        self.config = cfg
        time = self.config.next_time_slot
        self.config.next_time_slot = time + 1
        self.save_config()

        return time

    def get_history(
        self,
        dates: Optional[tuple[datetime, datetime]] = None,
        bounds: Optional[Bounds] = None,
        name: Optional[str] = None,
        concise: bool = False,
    ):
        """
        Retrieve history of the database at current point in time.

        :param dates: Query parameter, tuple of start and end datetimes.
        :param bounds: Query parameter, bounds to query by.
        :param name: Query paramter, shatter process uuid., by default None
        :param concise: Whether or not to give shortened version of history.
        :return: Returns list of array fragments that meet query parameters.
        """
        if bounds is None:
            bounds = self.config.root

        m = []
        for idx in range(1, self.config.next_time_slot):
            s = self.get_shatter_meta(idx)
            if s.bounds.disjoint(bounds):
                continue

            # filter name
            if name is not None and name != s.name:
                continue

            # filter dates
            start_ts = s.date[0].timestamp()
            end_ts = s.date[1].timestamp()

            if dates is not None:
                q_start_ts = dates[0].timestamp()
                q_end_ts = dates[0].timestamp()
                if not ts_overlap((q_start_ts, q_end_ts), (start_ts, end_ts)):
                    continue

            if concise:
                h = s.history_json()
            else:
                h = s.to_json()
            m.append(h)

        return m

    def mbrs(self, config: ShatterConfig):
        """
        Get minimum bounding rectangle of a given shatter process. If this
        process has been finished and consolidated the mbr will be much less
        granulated than if the fragments are still intact. Mbrs are represented
        as tuples in the form of ((minx, maxx), (miny, maxy))

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        """
        # TODO timestamp changes, check to make sure correct
        from .extents import Extents

        ex = Extents.from_sub(self, config.bounds)
        af_all = self.get_fragments(config.timestamp, config.bounds)
        mbrs_list = tuple(af.nonempty_domain for af in af_all)
        # mbrs = tuple(m for m in mbrs_list if not ex.disjoint_by_mbr(m))
        mbrs = tuple(
            tuple(
                tuple(a.item() for a in mb)
                for mb in m
            )
            for m in mbrs_list if not ex.disjoint_by_mbr(m)
        )
        return mbrs

    def get_fragments(
        self, timestamp: tuple[int, int], bounds: Optional[Bounds] = None
    ) -> list[tiledb.FragmentInfo]:
        """
        Get TileDB array fragments from the time slot specified.

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :return: Array fragments from time slot.
        """
        from .extents import Extents

        af = tiledb.array_fragments(self.config.tdb_dir, include_mbrs=True)
        if bounds is not None:
            ex = Extents.from_sub(self, bounds)
        fragments = []
        for a in af:
            if not ts_overlap(a.timestamp_range, timestamp):
                continue
            if bounds is not None:
                if a.mbrs:
                    if all(ex.disjoint_by_mbr(mbr) for mbr in a.mbrs):
                        continue
                elif a.nonempty_domain:
                    if ex.disjoint_by_mbr(a.nonempty_domain):
                        continue
            fragments.append(a)
        return fragments

        # return [a for a in af if ts_overlap(a.timestamp_range, timestamp)]

    def delete(self, config: ShatterConfig) -> ShatterConfig:
        """
        Delete Shatter process and all associated data from database.

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :return: Config of deleted Shatter process
        """

        self.config.log.debug(f'Deleting shatter process {config.name}...')
        vfs = tiledb.VFS(ctx=self.get_tdb_context())
        fragments = self.get_fragments(
            timestamp=config.timestamp, bounds=config.bounds
        )
        for fragment in fragments:
            parsed = urlparse(fragment.uri)
            sans_protocol = parsed.path
            fragname = os.path.basename(sans_protocol) + '.wrt'

            frag_path = Path(sans_protocol)
            commit_path = frag_path / '..' / '..' / '__commits' / fragname

            vfs.remove_dir(frag_path)
            vfs.remove_file(commit_path)

        r = self.open('r')
        sh_cfg = ShatterConfig.from_string(
            r.meta[f'shatter_{config.time_slot}']
        )
        sh_cfg.mbr = ()
        sh_cfg.finished = False
        sh_cfg.start_timestamp = None
        sh_cfg.end_timestamp = None

        # self.config.log.debug('Overwriting data.')
        # # TODO timestamp change: update config with newest timestamp after
        # # rewriting everything to null
        # tsr = self.open('r', timestamp=sh_cfg.timestamp)
        # indices = tsr.query(
        #     coords=True, cond=f'shatter_process_num=={time_slot}'
        # ).df[:]
        # # query returns tiles, so may still need to parse it down afterward
        # del_data = indices[indices.shatter_process_num == time_slot].set_index(
        #     ['X', 'Y']
        # )
        # dd = pd.DataFrame(
        #     columns=del_data.columns, index=del_data.index
        # ).reset_index()
        # self.write(dd, sh_cfg.timestamp)

        # self.config.log.debug('Rewriting config.')
        with self.open('w') as w:
            w.meta[f'shatter_{config.time_slot}'] = json.dumps(sh_cfg.to_json())

        # # loop until it's been updated?
        # self.consolidate(timestamp=sh_cfg.timestamp)
        # self.vacuum()
        # import time

        # count = 0
        # # TODO I don't like this, but need to make sure that the db has been
        # # updated before moving on
        # while True:
        #     with tiledb.open(self.config.tdb_dir, mode='r') as r:
        #         test_vals = r.query(
        #             coords=True, cond=f'shatter_process_num=={time_slot}'
        #         ).df[:]
        #         if test_vals[test_vals.shatter_process_num == time_slot].empty:
        #             break
        #         count = count + 1
        #         print(f'retry {count}')
        #         time.sleep(1)
        #         r.close()

        return sh_cfg

    ManageType = Union[
        Literal['fragments', 'fragment_meta', 'commits', 'array_meta']
    ]

    def vacuum(self, mode: ManageType = 'fragments'):
        c = tiledb.Config(
            {
                'sm.vacuum.mode': mode,
            }
        )
        tiledb.vacuum(self.config.tdb_dir, config=c)
        self.config.log.debug('Vacuuming complete.')

    def consolidate(
        self,
        timestamp: Optional[tuple[int, int]] = None,
        mode: Optional[ManageType] = 'fragments',
    ) -> None:
        """
        Consolidate the fragments from a shatter process into one fragment.
        This makes the database perform better, but reduces the granularity of
        time traveling.

        :param timestamp: TileDB timestamp, a tuple of start and end datetime.
        :param mode: TileDB consolidation mode.
        """
        ts_start = timestamp[0] if timestamp is not None else 0
        ts_end_def = int(datetime.now().timestamp()*1000)
        ts_end = timestamp[1] if timestamp is not None else ts_end_def
        c = tiledb.Config(
            {
                'sm.consolidation.mode': mode,
                'sm.consolidation.timestamp_start': ts_start,
                'sm.consolidation.timestamp_end': ts_end,
                'sm.consolidation.max_fragment_size': (300 * 2**20),  # 300MB
            }
        )
        tiledb.consolidate(
            self.config.tdb_dir,
            ctx=tiledb.Ctx(c),
            config=c
        )
        self.config.log.debug(f'Consolidated time slot {timestamp}.')
