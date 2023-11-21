import pyproj
import json
import copy
from dask.distributed import Client
from redis import Redis

from dataclasses import dataclass, field

from .names import get_random_name
from .bounds import Bounds
from . import __version__

    # config = Configuration(tdb_filepath, resolution, b, crs = crs, attrs = attrs)
@dataclass
class Configuration:
    tdb_dir: str
    bounds: Bounds
    resolution: float = 30.0
    crs: pyproj.CRS = None
    attrs: list[str] = field(default_factory=lambda:[ 'Z', 'NumberOfReturns', 'ReturnNumber', 'Intensity' ])
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

    def to_json(self):
        # silliness because pyproj.CRS doesn't default to using to_json
        d = copy.deepcopy(self.__dict__)
        d['crs'] = json.loads(self.crs.to_json())
        d['bounds'] = json.loads(self.bounds.to_json())
        j = json.dumps(d)
        return j

    @classmethod
    def from_string(cls, data: str):
        x = json.loads(data)
        bounds = Bounds(*x['bounds'])
        if 'crs' in x:
            crs = pyproj.CRS.from_user_input(json.dumps(x['crs']))
        else:
            crs = None
        n = cls(x['tdb_dir'], bounds, x['resolution'], attrs=x['attrs'], crs=crs)

        return n

    def __repr__(self):
        j = self.to_json()
        return json.dumps(j)

@dataclass(kw_only=True)
class ShatterConfiguration:

    tdb_dir: str
    filename: str
    tile_size: int
    debug: bool=field(default=False)
    client: Client=field(default=None)
    redis_url: str=field(default=None)

    def __post_init__(self) -> None:
        if self.redis_url is not None:
            r = Redis.from_url(self.redis_url)
            try:
                r.ping()
            except BaseException as e:
                raise Exception(f"Invalid redis url provided: {e.args}")
        if self.client is not None:
            # throws if not all package versions found on client workers match
            self.client.get_versions(check=True)