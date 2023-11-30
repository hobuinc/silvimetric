import pyproj
import json
import copy
from dask.distributed import Client

from dataclasses import dataclass, field

from .names import get_random_name
from .bounds import Bounds
from .metric import Metric, Metrics
from . import __version__

    # config = Configuration(tdb_filepath, resolution, b, crs = crs, attrs = attrs)
@dataclass
class Configuration:
    tdb_dir: str
    bounds: Bounds
    resolution: float = 30.0
    crs: pyproj.CRS = None
    attrs: list[str] = field(default_factory=lambda:[ 'Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity' ])
    metrics: list[str] = field(default_factory=lambda: [m for m in Metrics.keys()])
    version: str = __version__
    name: str = None

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

        if not self.name:
            name = get_random_name()

        self.metric_definitions = { m: str(Metrics[m]) for m in self.metrics}


    def to_json(self):
        # silliness because pyproj.CRS doesn't default to using to_json
        d = copy.deepcopy(self.__dict__)
        d['crs'] = json.loads(self.crs.to_json())
        d['bounds'] = json.loads(self.bounds.to_json())
        return d

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        bounds = Bounds(*x['bounds'])
        if 'crs' in x:
            crs = pyproj.CRS.from_user_input(json.dumps(x['crs']))
        else:
            crs = None
        n = cls(x['tdb_dir'], bounds, x['resolution'], attrs=x['attrs'],
                crs=crs, metrics=x['metrics'])

        return n

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class ShatterConfiguration:
    tdb_dir: str
    filename: str
    tile_size: int
    attrs: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    debug: bool=field(default=False)
    # pipeline: str=field(default=None)
    point_count: int=field(default=0)

    def __post_init__(self) -> None:
        from .storage import Storage
        #TODO should storage be a member variable?
        s = Storage.from_db(self.tdb_dir)
        if not self.attrs:
            self.attrs = s.getAttributes()
        if not self.metrics:
            self.metrics = s.getMetrics()

    def to_json(self):
        meta = {}
        meta['filename'] = self.filename
        meta['tile_size'] = self.tile_size
        meta['point_count'] = self.point_count
        # meta['pipeline'] = self.pipeline
        return meta

    def __repr__(self):
        return json.dumps(self.to_json())

@dataclass
class ExtractConfiguration:
    tdb_dir: str
    out_dir: str
    attrs: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        from .storage import Storage
        config = Storage.from_db(self.tdb_dir).config
        if not self.attrs:
            self.attrs = config.attrs()
        if not self.metrics:
            self.metrics = config.metrics()

        self.bounds: Bounds = config.bounds
        self.resolution: float = config.resolution
        self.crs: pyproj.CRS = config.crs