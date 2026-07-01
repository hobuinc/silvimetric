import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import zarr


@dataclass(frozen=True)
class ZarrDim:
    name: str
    domain: tuple[int, int]
    tile: int
    dtype: np.dtype = np.dtype(np.uint64)

    def __post_init__(self):
        object.__setattr__(self, 'dtype', np.dtype(self.dtype))

    def to_json(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'domain': list(self.domain),
            'tile': self.tile,
            'dtype': self.dtype.str,
        }

    @staticmethod
    def from_json(data: dict[str, Any]) -> 'ZarrDim':
        return ZarrDim(
            name=data['name'],
            domain=tuple(data['domain']),
            tile=data['tile'],
            dtype=np.dtype(data['dtype']),
        )


@dataclass(frozen=True)
class ZarrAttr:
    name: str
    dtype: np.dtype
    var: bool = False
    nullable: bool = False
    fill: Any = None

    def __post_init__(self):
        object.__setattr__(self, 'dtype', np.dtype(self.dtype))

    def to_json(self) -> dict[str, Any]:
        fill = self.fill
        if isinstance(fill, np.datetime64):
            fill = str(fill)
        elif isinstance(fill, np.generic):
            fill = fill.item()
        return {
            'name': self.name,
            'dtype': self.dtype.str,
            'var': self.var,
            'nullable': self.nullable,
            'fill': fill,
        }

    @staticmethod
    def from_json(data: dict[str, Any]) -> 'ZarrAttr':
        return ZarrAttr(
            name=data['name'],
            dtype=np.dtype(data['dtype']),
            var=data.get('var', False),
            nullable=data.get('nullable', False),
            fill=data.get('fill'),
        )


class ZarrDomain:
    def __init__(self, dims: list[ZarrDim]):
        self._dims = {d.name: d for d in dims}

    def dim(self, name: str) -> ZarrDim:
        return self._dims[name]

    def to_json(self) -> list[dict[str, Any]]:
        return [d.to_json() for d in self._dims.values()]

    @staticmethod
    def from_json(data: list[dict[str, Any]]) -> 'ZarrDomain':
        return ZarrDomain([ZarrDim.from_json(d) for d in data])


class ZarrSchema:
    def __init__(self, domain: ZarrDomain, attrs: list[ZarrAttr]):
        self.domain = domain
        self._attrs = {a.name: a for a in attrs}

    def has_attr(self, name: str) -> bool:
        return name in self._attrs

    def attr(self, name: str) -> ZarrAttr:
        return self._attrs[name]

    def check(self) -> None:
        return None

    def to_json(self) -> str:
        return json.dumps(
            {
                'domain': self.domain.to_json(),
                'attrs': [a.to_json() for a in self._attrs.values()],
            }
        )

    @staticmethod
    def from_json(data: str | dict[str, Any]) -> 'ZarrSchema':
        if isinstance(data, str):
            data = json.loads(data)
        return ZarrSchema(
            domain=ZarrDomain.from_json(data['domain']),
            attrs=[ZarrAttr.from_json(a) for a in data['attrs']],
        )


@dataclass(frozen=True)
class ZarrFragment:
    timestamp_range: tuple[int, int]
    nonempty_domain: tuple[tuple[int, int], tuple[int, int]]
    mbrs: tuple[tuple[tuple[int, int], tuple[int, int]], ...] = tuple()


class ZarrDataFrameAccessor:
    def __init__(self, array: 'ZarrArray', attrs: list[str] | None = None):
        self.array = array
        self.attrs = attrs

    def __getitem__(self, key):
        df = self.array.to_dataframe()
        if isinstance(key, tuple):
            xkey, ykey = key
            df = _slice_dim(df, 'X', xkey)
            df = _slice_dim(df, 'Y', ykey)
        elif isinstance(key, slice):
            if key != slice(None, None, None):
                df = _slice_dim(df, 'X', key)
        if self.attrs is not None:
            keep = [*self.attrs, 'X', 'Y']
            keep = [c for c in keep if c in df.columns]
            df = df[keep]
        return df


class ZarrQuery:
    def __init__(self, array: 'ZarrArray', attrs: list[str] | None = None):
        self.df = ZarrDataFrameAccessor(array, attrs)


class ZarrArray:
    def __init__(self, uri: str, mode: str = 'r', timestamp=None):
        self.uri = uri
        self.mode = mode
        self.timestamp = timestamp
        self.group = zarr.open_group(uri, mode='a')
        self.meta = self.group.attrs
        self.schema = ZarrSchema.from_json(self.group.attrs['schema'])
        self.df = ZarrDataFrameAccessor(self)
        self.isopen = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()
        return None

    def close(self):
        self.isopen = False

    def reopen(self):
        self.group = zarr.open_group(self.uri, mode='a')
        self.meta = self.group.attrs
        self.isopen = True

    def query(self, attrs=None, **_kwargs):
        return ZarrQuery(self, attrs)

    def __getitem__(self, key):
        return self.df[key]

    def __setitem__(self, key, data):
        x, y = key
        record = {'X': x, 'Y': y}
        for name, value in data.items():
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            record[name] = value
        append_records(self.uri, pd.DataFrame([record]))

    def to_dataframe(self) -> pd.DataFrame:
        df = read_records(self.uri)
        if df.empty:
            return _empty_frame(self.schema)
        if self.timestamp is not None and '_commit_ms' in df.columns:
            df = df[df['_commit_ms'] <= self.timestamp[1]]
        df = df.sort_values('_write_id')
        df = df.drop_duplicates(['X', 'Y'], keep='last')
        return df.reset_index(drop=True)


def create_store(uri: str, schema: ZarrSchema, metadata: dict[str, Any]) -> None:
    Path(uri).mkdir(parents=True, exist_ok=True)
    group = zarr.open_group(uri, mode='w')
    group.attrs['_silvimetric_backend'] = 'zarr'
    group.attrs['schema'] = schema.to_json()
    for key, value in metadata.items():
        group.attrs[key] = value
    _write_blob(group, pd.DataFrame())


def is_store(uri: str) -> bool:
    path = Path(uri)
    if not path.exists():
        return False
    try:
        group = zarr.open_group(uri, mode='r')
    except Exception:
        return False
    return group.attrs.get('_silvimetric_backend') == 'zarr'


def open_array(uri: str, mode: str = 'r', timestamp=None) -> ZarrArray:
    return ZarrArray(uri, mode=mode, timestamp=timestamp)


def append_records(uri: str, data: pd.DataFrame) -> None:
    if data.empty:
        return
    group = zarr.open_group(uri, mode='a')
    existing = _read_blob(group)
    write_id = int(group.attrs.get('_next_write_id', 1))
    data = data.copy()
    now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    data['_write_id'] = write_id
    data['_commit_ms'] = now_ms
    merged = pd.concat([existing, data], ignore_index=True)
    _write_blob(group, merged)
    group.attrs['_next_write_id'] = write_id + 1


def read_records(uri: str) -> pd.DataFrame:
    group = zarr.open_group(uri, mode='a')
    return _read_blob(group)


def fragments(uri: str, timestamp: tuple[int, int] | None = None):
    df = read_records(uri)
    if df.empty:
        return []
    if timestamp is not None:
        df = df[df['_commit_ms'] <= timestamp[1]]
    frags = []
    for _write_id, part in df.groupby('_write_id'):
        xs = (int(part.X.min()), int(part.X.max()))
        ys = (int(part.Y.min()), int(part.Y.max()))
        commit = int(part['_commit_ms'].max())
        domain = (xs, ys)
        frags.append(ZarrFragment((commit, commit), domain, (domain,)))
    return frags


def _slice_dim(df: pd.DataFrame, name: str, key):
    if isinstance(key, slice):
        start = key.start
        stop = key.stop
        if start is not None:
            df = df[df[name] >= start]
        if stop is not None:
            df = df[df[name] <= stop]
        return df
    return df[df[name] == key]


def _empty_frame(schema: ZarrSchema) -> pd.DataFrame:
    return pd.DataFrame(columns=['X', 'Y', *schema._attrs.keys()])


def _write_blob(group, df: pd.DataFrame) -> None:
    payload = pickle.dumps(df)
    data = np.frombuffer(payload, dtype=np.uint8)
    group.create_array('records_blob', data=data, overwrite=True)


def _read_blob(group) -> pd.DataFrame:
    if 'records_blob' not in group:
        return pd.DataFrame()
    payload = bytes(group['records_blob'][:])
    if not payload:
        return pd.DataFrame()
    return pickle.loads(payload)
