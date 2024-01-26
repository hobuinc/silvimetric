import pyproj

import json
import uuid
import numpy as np

from pathlib import Path
from abc import ABC, abstractmethod
from typing import Union, Tuple
from datetime import datetime

from dataclasses import dataclass, field

from .log import Log
from .extents import Bounds
from .metric import Metric, Metrics
from .entry import Attribute, Attributes
from . import __version__


class SilviMetricJSONEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__

@dataclass(kw_only=True)
class Config(ABC):
    tdb_dir: str
    log: Log = field(default_factory = lambda: Log("INFO"))
    debug: bool = field(default=False)

    def to_json(self):
        keys = self.__dataclass_fields__.keys()
        d = {}
        for k in keys:
            d[k] = self.__dict__[k]
        if not isinstance(d['log'], dict):
            d['log'] = d['log'].to_json()
        return d

    @classmethod
    @abstractmethod
    def from_string(self, data: str):
        raise NotImplementedError

    def __repr__(self):
        return json.dumps(self.to_json())




@dataclass
class StorageConfig(Config):
    root: Bounds
    crs: pyproj.CRS
    resolution: float = 30.0

    attrs: list[Attribute] = field(default_factory=lambda: [
        Attribute(a, Attributes[a].dtype)
        for a in [ 'Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity' ]])
    metrics: list[Metric] = field(default_factory=lambda: [ Metrics[m]
                                  for m in Metrics.keys() ])
    version: str = __version__
    capacity: int = 1000000


    def __post_init__(self) -> None:

        crs = self.crs
        if isinstance(crs, dict):
            crs = json.loads(crs)
        elif isinstance(crs, pyproj.CRS):
            self.crs = crs
        else:
            self.crs = pyproj.CRS.from_user_input(crs)
        if not len(self.attrs):
            self.attrs = [Attributes[a] for a in [ 'Z', 'NumberOfReturns',
                                            'ReturnNumber', 'Intensity' ]]
        if not len(self.metrics):
            self.metrics = [ Metrics[m] for m in Metrics.keys() ]

        if not self.crs.is_projected:
            raise Exception(f"Given coordinate system is not a rectilinear projected coordinate system")

        self.metric_definitions = { m.name: str(m) for m in self.metrics}

    def __eq__(self, other):

        # We don't compare logs
        eq = True
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
        d['root'] = json.loads(self.root.to_json())
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        root = Bounds(*x['root'])
        if 'metrics' in x:
            ms = [ Metric.from_string(m) for m in x['metrics']]
        else:
            ms = None
        if 'attrs' in x:
            attrs = [ Attribute.from_string(a) for a in x['attrs']]
        else:
            attrs = None
        if 'crs' in x:
            crs = pyproj.CRS.from_user_input(json.dumps(x['crs']))
        else:
            crs = None
        n = cls(tdb_dir = x['tdb_dir'],
                root = root,
                log = Log(**x['log']),
                resolution = x['resolution'],
                attrs = attrs,
                crs = crs,
                metrics = ms,
                capacity = x['capacity'])

        return n

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class ApplicationConfig(Config):
    debug: bool = False,
    progress: bool = False,

    def to_json(self):
        d = super().to_json()
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        n = cls(tdb_dir = x['tdb_dir'],
                debug = x['debug'],
                progress = x['progress'])
        return n

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class ShatterConfig(Config):
    filename: str
    date: Union[datetime, Tuple[datetime, datetime]]
    attrs: list[Attribute] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)
    bounds: Union[Bounds, None] = field(default=None)
    name: uuid.UUID = field(default=uuid.uuid4())
    tile_size: Union[int, None] = field(default=None)
    point_count: int = field(default=0)
    nonempty_domain: tuple[tuple[int, int], ...] = field(default=())
    finished: bool = field(default=False)

    def __post_init__(self) -> None:
        from .storage import Storage
        s = Storage.from_db(self.tdb_dir)

        if not self.attrs:
            self.attrs = s.getAttributes()
        if not self.metrics:
            self.metrics = s.getMetrics()

        del s

    def to_json(self):
        d = super().to_json()

        d['name'] = str(self.name)
        d['bounds'] = json.loads(self.bounds.to_json()) if self.bounds is not None else None
        d['attrs'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        if isinstance(self.date, tuple):
            d['date'] = [ dt.strftime('%Y-%m-%dT%H:%M:%SZ') for dt in self.date]
        else:
            d['date'] = self.date.strftime('%Y-%m-%dT%H:%M:%SZ')
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)

        ms = list([ Metric.from_string(m) for m in x['metrics']])
        attrs = list([ Attribute.from_string(a) for a in x['attrs']])
        if isinstance(x['date'], list):
            date = tuple(( datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ') for d in x['date']))
        else:
            date = datetime.strptime(x['date'], '%Y-%m-%dT%H:%M:%SZ')
        # TODO key error if these aren't there. If we're calling from_string
        # then these keys need to exist.

        n = cls(tdb_dir = x['tdb_dir'],
                filename = x['filename'],
                attrs = attrs,
                metrics = ms,
                debug = x['debug'],
                name = uuid.UUID(x['name']),
                bounds=Bounds(*x['bounds']),
                date= date)

        return n

    def __repr__(self):
        return json.dumps(self.to_json())


@dataclass
class ExtractConfig(Config):
    out_dir: str
    attrs: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    bounds: Bounds = field(default=None)

    def __post_init__(self) -> None:
        from .storage import Storage
        config = Storage.from_db(self.tdb_dir).config
        if not len(self.attrs):
            self.attrs = config.attrs
        if not len(self.metrics):
            self.metrics = config.metrics
        if self.bounds is None:
            self.bounds: Bounds = config.root

        p = Path(self.out_dir)
        p.mkdir(parents=True, exist_ok=True)

        self.resolution: float = config.resolution
        self.crs: pyproj.CRS = config.crs

    def to_json(self):
        d = super().to_json()

        d['metrics'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        d['crs'] = json.loads(self.crs.to_json())
        d['bounds'] = json.loads(self.bounds.to_json())
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        if 'metrics' in x:
            ms = [ Metric.from_string(m) for m in x['metrics']]
        if 'attrs' in x:
            attrs = [ Attribute.from_string(a) for a in x['attrs']]
        n = cls(x['out_dir'], attrs, ms, x['debug'])

        return n

    def __repr__(self):
        return json.dumps(self.to_json())