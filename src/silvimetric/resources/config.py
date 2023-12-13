import pyproj
import json
import copy
import uuid
import pdal
from pathlib import Path
from abc import ABC, abstractmethod

from dataclasses import dataclass, field

# from .names import get_random_name
from .extents import Bounds
from .metric import Metric, Metrics, Attribute
from . import __version__

@dataclass
class Config(ABC):
    tdb_dir: str

    @abstractmethod
    def to_json(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_string(self, data: str):
        raise NotImplementedError

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class StorageConfig(Config):
    bounds: Bounds
    resolution: float = 30.0
    crs: pyproj.CRS = None
    #TODO change these to a list of Metric and Entry class objects

    def attr_make():
        dims = { d['name']: d['dtype'] for d in pdal.dimensions }
        return [ Attribute(a, dims[a])
        for a in [ 'Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity' ] ]

    attrs: list[Attribute] = field(default_factory=attr_make)

    metrics: list[Metric] = field(default_factory=lambda: [ Metrics[m]
                                  for m in Metrics.keys() ])
    version: str = __version__

    def __post_init__(self) -> None:

        crs = self.crs
        if isinstance(crs, dict):
            crs = json.loads(crs)
        elif isinstance(crs, pyproj.CRS):
            self.crs = crs
        else:
            self.crs = pyproj.CRS.from_user_input(crs)

        if not self.crs.is_projected:
            raise Exception(f"Given coordinate system is not a rectilinear projected coordinate system")

        self.metric_definitions = { m.name: str(m) for m in self.metrics}


    def to_json(self):
        # silliness because pyproj.CRS doesn't default to using to_json
        d = copy.deepcopy(self.__dict__)

        d['attrs'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        d['crs'] = json.loads(self.crs.to_json())
        d['bounds'] = json.loads(self.bounds.to_json())
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        bounds = Bounds(*x['bounds'])
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
        n = cls(x['tdb_dir'], bounds, x['resolution'], attrs=attrs,
                crs=crs, metrics=ms)

        return n

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class ShatterConfig(Config):
    filename: str
    tile_size: int
    attrs: list[Attribute] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)
    debug: bool = field(default=False)
    name: uuid.UUID = field(default=uuid.uuid1())
    bounds: Bounds = field(default=None)
    # pipeline: str=field(default=None)

    def __post_init__(self) -> None:
        from .storage import Storage
        s = Storage.from_db(self.tdb_dir)
        if self.attrs is None:
            self.attrs = s.getAttributes()
        if self.metrics is None:
            self.metrics = s.getMetrics()
        if self.bounds is None:
            self.bounds = s.config.bounds
        self.point_count: int = 0

    def to_json(self):
        d = copy.deepcopy(self.__dict__)
        d['name'] = str(self.name)
        d['bounds'] = json.loads(self.bounds.to_json())
        d['attrs'] = [a.to_json() for a in self.attrs]
        d['metrics'] = [m.to_json() for m in self.metrics]
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)

        ms = [ Metric.from_string(m) for m in x['metrics']]
        attrs = [ Attribute.from_string(a) for a in x['attrs']]
        # TODO key error if these aren't there. If we're calling from_string
        # then these keys need to exist.

        n = cls(tdb_dir=x['tdb_dir'], filename=x['filename'],
                tile_size=x['tile_size'], attrs=attrs, metrics=ms,
                debug=x['debug'], name=uuid.UUID(x['name']))

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
        if self.attrs is None:
            self.attrs = config.attrs
        if self.metrics is None:
            self.metrics = config.metrics
        if self.bounds is None:
            self.bounds: Bounds = config.bounds

        p = Path(self.out_dir)
        p.mkdir(parents=True, exist_ok=True)

        self.resolution: float = config.resolution
        self.crs: pyproj.CRS = config.crs

    def to_json(self):
        # silliness because pyproj.CRS doesn't default to using to_json
        d = copy.deepcopy(self.__dict__)
        d['attrs'] = [a.to_json() for a in self.attrs]
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
        n = cls(x['out_dir'], x['tile_size'], attrs, ms, x['debug'])

        return n

    def __repr__(self):
        return json.dumps(self.to_json())