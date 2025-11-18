import pyproj

import os
import json
import uuid

from pathlib import Path
from abc import ABC, abstractmethod
from typing_extensions import Union, Tuple
from datetime import datetime

from dataclasses import dataclass, field

from .log import Log
from .extents import Bounds
from .metric import Metric
from .metrics import grid_metrics
from .attribute import Attribute, Attributes
from .. import __version__

from threading import Lock
mutex = Lock()


@dataclass(kw_only=True)
class Config(ABC):
    """Base config"""

    tdb_dir: str = field()
    """Path to TileDB directory to use."""
    log: Log = field(default_factory=lambda: Log('INFO'))
    """Log object."""
    debug: bool = field(default=False)
    """Debug flag."""

    def to_json(self):
        keys = self.__dataclass_fields__.keys()
        d = {}
        for k in keys:
            if k == 'tdb_dir':
                tdb_dir = self.__dict__[k]
                if '://' not in tdb_dir:
                    d[k] = os.path.abspath(tdb_dir)
                else:
                    d[k] = tdb_dir
            else:
                d[k] = self.__dict__[k]
        if not isinstance(d['log'], dict):
            d['log'] = d['log'].to_json()
        return d

    @classmethod
    def from_json(self, data: str):
        return self.from_string(json.dumps(data))

    @classmethod
    @abstractmethod
    def from_string(self, data: str):
        raise NotImplementedError

    def __repr__(self):
        return json.dumps(self.to_json())


@dataclass
class StorageConfig(Config):
    """Config for constructing a Storage object"""
    root: Bounds = field()
    """Root project bounding box"""
    crs: pyproj.CRS = field()
    """Coordinate reference system, same for all data in a project"""
    resolution: float = field(default=30.0)
    """Resolution of cells, same for all data in a project, defaults to 30.0"""
    alignment: str = field(default='AlignToCenter')
    """Alignment of pixels in database, same for all data in a project,
    options: 'AlignToCenter' or 'AlignToCorner', defaults to 'AlignToCenter'"""
    xsize: int = field(default=1000)
    """TileDB X Tile size for IO operations."""
    ysize: int = field(default=1000)
    """TileDB Y Tile size for IO operations."""

    attrs: list[Attribute] = field(
        default_factory=lambda: [
            Attribute(a, Attributes[a].dtype)
            for a in ['Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity']
        ]
    )
    """List of :class:`silvimetric.resources.attribute.Attribute` attributes
    that represent point data, defaults to Z, NumberOfReturns, ReturnNumber,
    Intensity"""
    metrics: list[Metric] = field(
        default_factory=lambda: list(grid_metrics.get_grid_metrics().values())
    )
    """List of :class:`silvimetric.resources.metrics.grid_metrics` grid_metrics
    that represent derived data, defaults to values in grid_metrics object"""
    version: str = field(default=__version__)
    """Silvimetric version"""
    capacity: int = field(default=1000000)
    """TileDB Capacity, defaults to 1000000"""
    next_time_slot: int = field(default=1)
    """Next time slot to be allocated to a shatter process. Increment after
    use., defaults to 1"""

    def __post_init__(self) -> None:
        crs = self.crs
        if isinstance(crs, dict):
            crs = json.loads(crs)
        elif isinstance(crs, pyproj.CRS):
            self.crs = crs
        else:
            self.crs = pyproj.CRS.from_user_input(crs)

        if not self.crs.is_projected:
            raise Exception(
                'Given coordinate system is not a rectilinear'
                ' projected coordinate system'
            )

    def __eq__(self, other):
        # We don't compare logs
        for k in other.__dict__.keys():
            if k != 'log':
                if self.__dict__[k] != other.__dict__[k]:
                    return False
        return True

    def to_json(self):
        d = super().to_json()

        d['attrs'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        d['crs'] = json.loads(self.crs.to_json())
        d['root'] = self.root.to_json()
        d['xsize'] = self.xsize
        d['ysize'] = self.ysize
        d['next_time_slot'] = self.next_time_slot

        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        root = Bounds(*x['root'])
        if 'metrics' in x:
            with mutex:
                ms = [Metric.from_dict(m) for m in x['metrics']]
        else:
            ms = []
        if 'attrs' in x:
            attrs = [Attribute.from_dict(a) for a in x['attrs']]
        else:
            attrs = []
        if 'crs' in x:
            crs = pyproj.CRS.from_user_input(json.dumps(x['crs']))
        else:
            crs = None

        n = cls(
            tdb_dir=x['tdb_dir'],
            root=root,
            log=Log(**x['log']),
            resolution=x['resolution'],
            alignment=x['alignment'],
            attrs=attrs,
            crs=crs,
            metrics=ms,
            capacity=x['capacity'],
            version=x['version'],
            next_time_slot=x['next_time_slot'],
            xsize = x['xsize'],
            ysize = x['ysize']
        )

        return n

    def __repr__(self):
        j = self.to_json()
        return json.dumps(j)


@dataclass
class ApplicationConfig(Config):
    """Base application config"""

    debug: bool = field(default=False)
    """Debug mode, defaults to False"""

    # Dask configuration
    dasktype: str = field(default='processes')
    """Dask parallelization type. For information see
    https://docs.dask.org/en/stable/scheduling.html#local-threads """
    scheduler: str = field(default='distributed')
    """Dask scheduler, defaults to 'distributed'"""
    workers: int = field(default=12)
    """Number of dask workers"""
    threads: int = field(default=4)
    """Number of threads per dask worker"""
    watch: bool = field(default=False)
    """Open dask diagnostic page in default web browser"""

    def to_json(self):
        d = super().to_json()
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        n = cls(
            tdb_dir=x['tdb_dir'],
            debug=x['debug'],
            dasktype=x['dasktype'],
            scheduler=x['scheduler'],
            workers=x['workers'],
            threads=x['threads'],
            watch=x['watch'],
        )
        return n

    def __repr__(self):
        return json.dumps(self.to_json())


Mbr = tuple[tuple[tuple[int, int], tuple[int, int]], ...]


@dataclass
class ShatterConfig(Config):
    """Config for Shatter process"""

    filename: str
    """Input filename referencing a PDAL pipeline or point cloud file."""
    date: Tuple[datetime, datetime] = field(default=None)
    """A date range representing data collection times."""
    bounds: Union[Bounds, None] = field(default=None)
    """The bounding box of the shatter process., defaults to None"""
    name: uuid.UUID = field(default=uuid.uuid4())
    """UUID representing this shatter process and will be generated if not
    provided., defaults to uuid.uuid()"""
    tile_size: Union[int, None] = field(default=None)
    """The number of cells to include in a tile., defaults to None"""
    start_timestamp: float = field(default=None)
    """The process start timestamp., defaults to None"""
    end_timestamp: float = field(default=None)
    """The process ending timestamp., defaults to None"""
    point_count: int = field(default=0)
    """The number of points that has been processed so far., defaults to 0"""
    tile_point_count: int = field(default=600*10**3) #600k
    """Target number of points per Tile. Only used if tile_size is None.
    defaults to 600000"""
    mbr: Mbr = field(default_factory=lambda: tuple())
    """The minimum bounding rectangle derived from TileDB array fragments.
    This will be used to for resuming shatter processes and making sure it
    doesn't repeat work., defaults to tuple()"""
    finished: bool = False
    """Finished flag for shatter process., defaults to False"""
    time_slot: int = 0
    """The time slot that has been reserved for this shatter process. Will be
    used as an attribute in tiledb writes to better organize and manage
    processes., defaults to 0"""
    version: str = field(default=__version__)
    """SilviMetric Version"""

    def __post_init__(self) -> None:
        from .storage import Storage

        if isinstance(self.tdb_dir, Storage):
            self.tdb_dir = self.tdb_dir.config.tdb_dir
        if isinstance(self.date, datetime):
            self.date = (self.date, self.date)
        if isinstance(self.date, list):
            self.date = tuple(d for d in self.date)
        if len(self.date) > 2 or len(self.date) < 1:
            raise ValueError(
                f'Invalid date range ({self.date}). Must be either 1 or 2 values.'
            )
        if len(self.date) == 1:
            self.date = (self.date[0], self.date[0])

        if isinstance(self.tile_size, float):
            self.tile_size = int(self.tile_size)

    @property
    def timestamp(self):
        end_time_temp = int(datetime.now().timestamp()*1000)
        if self.start_timestamp is None:
            return tuple((0, end_time_temp))
        if self.end_timestamp is None:
            return tuple((self.start_timestamp, end_time_temp))
        else:
            return tuple((self.start_timestamp, self.end_timestamp))

    def history_json(self):
        # removing a attrs and metrics, since they'll be in the storage log
        # removing mbr because it's too big to do json pretty printing with
        # could add a custom json logger to handle mbr logging in the future
        date = (
            self.date[0].strftime('%Y-%m-%dT%H:%M:%SZ'),
            self.date[1].strftime('%Y-%m-%dT%H:%M:%SZ'),
        )
        d = dict(
            filename=self.filename,
            name=str(self.name),
            time_slot=self.time_slot,
            bounds=self.bounds.to_json(),
            date=date,
        )

        return d

    def to_json(self):
        d = super().to_json()

        d['name'] = str(self.name)
        d['time_slot'] = self.time_slot
        d['bounds'] = self.bounds.to_json() if self.bounds is not None else None
        d['mbr'] = list(self.mbr)
        d['date'] = [dt.strftime('%Y-%m-%dT%H:%M:%SZ') for dt in self.date]

        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        return cls.from_dict(x)

    @classmethod
    def from_dict(cls, data: dict):
        x = data

        if isinstance(x['date'], list):
            date = tuple(
                (datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ') for d in x['date'])
            )
        else:
            date = datetime.strptime(x['date'], '%Y-%m-%dT%H:%M:%SZ')
        mbr = tuple(tuple(tuple(mb) for mb in m) for m in x['mbr'])
        # TODO key error if these aren't there. If we're calling from_string
        # then these keys need to exist.

        n = cls(
            tdb_dir=x['tdb_dir'],
            filename=x['filename'],
            debug=x['debug'],
            name=uuid.UUID(x['name']),
            bounds=Bounds(*x['bounds']),
            tile_size=x['tile_size'],
            start_timestamp=x['start_timestamp'],
            end_timestamp=x['end_timestamp'],
            point_count=x['point_count'],
            mbr=mbr,
            date=date,
            time_slot=x['time_slot'],
            finished=x['finished'],
        )

        return n

    def __repr__(self):
        return json.dumps(self.to_json())


@dataclass
class ExtractConfig(Config):
    """Config for the Extract process."""

    out_dir: str
    """The directory where derived rasters should be written."""
    attrs: list[Attribute] = field(default=None)
    """List of attributes to use in shatter. If this is not set it
    will be filled by the attributes in the database instance."""
    metrics: list[Metric] = field(default=None)
    """A list of metrics to use in shatter. If this is not set it
    will be filled by the metrics in the database instance."""
    bounds: Bounds = field(default=None)
    """The bounding box of the shatter process., defaults to None"""
    date: Tuple[datetime, datetime] = field(
        default_factory=lambda : tuple([
            datetime(1970, 1, 1), datetime.now()
        ])
    )
    """A date range representing data collection times."""

    def __post_init__(self) -> None:
        from .storage import Storage

        if isinstance(self.tdb_dir, Storage):
            config = self.tdb_dir.config
            self.tdb_dir = config.tdb_dir
        else:
            config = Storage.from_db(self.tdb_dir).config

        if self.attrs is None:
            self.attrs = config.attrs
        if self.metrics is None:
            self.metrics = config.metrics
        if self.bounds is None:
            self.bounds: Bounds = config.root

        if isinstance(self.date, datetime):
            self.date = (self.date, self.date)
        if isinstance(self.date, list):
            self.date = tuple(d for d in self.date)
        if len(self.date) > 2 or len(self.date) < 1:
            raise ValueError(
                f'Invalid date range ({self.date}). Must be either 1 or 2 values.'
            )
        if len(self.date) == 1:
            self.date = (self.date[0], self.date[0])

        p = Path(self.out_dir)
        p.mkdir(parents=True, exist_ok=True)

        self.resolution: float = config.resolution
        self.crs: pyproj.CRS = config.crs

    def to_json(self):
        d = super().to_json()

        d['attrs'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        d['crs'] = json.loads(self.crs.to_json())
        d['bounds'] = self.bounds.to_json()
        d['date'] = [dt.strftime('%Y-%m-%dT%H:%M:%SZ') for dt in self.date]
        return d

    @classmethod
    def from_dict(cls, data: object):
        if 'metrics' in data:
            with mutex:
                ms = [Metric.from_dict(m) for m in data['metrics']]
        if 'attrs' in data:
            attrs = [Attribute.from_dict(a) for a in data['attrs']]
        if 'bounds' in data:
            bounds = Bounds(*data['bounds'])
        if 'log' in data:
            l = data['log']  # noqa: E741
            log = Log(
                l['log_level'], l['logdir'], l['logtype'], l['logfilename']
            )
        else:
            log = Log('INFO')
        if isinstance(data['date'], list):
            date = tuple(
                (datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ') for d in data['date'])
            )
        else:
            date = datetime.strptime(data['date'], '%Y-%m-%dT%H:%M:%SZ')

        return cls(
            tdb_dir=data['tdb_dir'],
            out_dir=data['out_dir'],
            attrs=attrs,
            metrics=ms,
            debug=data['debug'],
            bounds=bounds,
            log=log,
            date=date
        )


    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        return ExtractConfig.from_dict(x)

    def __repr__(self):
        return json.dumps(self.to_json())
