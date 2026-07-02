from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import tiledb

from .zarr_backend import ZarrSchema


def create_store(uri: str, schema: ZarrSchema, metadata: dict[str, Any]) -> None:
    tiledb_schema = _to_tiledb_schema(schema)
    tiledb_schema.check()
    tiledb.DenseArray.create(uri, tiledb_schema)
    with tiledb.DenseArray(uri, 'w') as writer:
        for key, value in metadata.items():
            writer.meta[key] = value
        writer.meta['_silvimetric_backend'] = 'tiledb'


def is_store(uri: str) -> bool:
    try:
        return tiledb.object_type(uri) == 'array'
    except Exception:
        return False


def open_array(uri: str, mode: str = 'r', timestamp=None):
    if timestamp is not None or mode != 'r':
        return tiledb.open(
            uri, mode, timestamp=timestamp, ctx=get_tdb_context()
        )
    return tiledb.open(uri, 'r')


def write_records(
    uri: str,
    data: pd.DataFrame,
    column_types: dict,
    varlen_types: set,
    fillna: dict,
) -> None:
    tiledb.from_pandas(
        uri=uri,
        sparse=False,
        dataframe=data,
        mode='append',
        column_types=column_types,
        varlen_types=varlen_types,
        fillna=fillna,
        fit_to_df=True,
    )


def fragments(uri: str, _timestamp: tuple[int, int] | None = None):
    return tiledb.array_fragments(uri, include_mbrs=True)


def vacuum(uri: str, mode: str = 'fragments'):
    config = tiledb.Config({'sm.vacuum.mode': mode})
    tiledb.vacuum(uri, config=config)


def consolidate(
    uri: str,
    mode: str = 'fragments',
    timestamp: tuple[int, int] | None = None,
) -> None:
    ts_start = timestamp[0] if timestamp is not None else 0
    ts_end_def = int(datetime.now().timestamp() * 1000)
    ts_end = timestamp[1] if timestamp is not None else ts_end_def
    config = tiledb.Config(
        {
            'sm.consolidation.mode': mode,
            'sm.consolidation.timestamp_start': ts_start,
            'sm.consolidation.timestamp_end': ts_end,
        }
    )
    tiledb.consolidate(uri, ctx=tiledb.Ctx(config), config=config)


def get_tdb_context():
    config = tiledb.Config()
    config['vfs.s3.connect_scale_factor'] = '25'
    config['vfs.s3.connect_max_retries'] = '10'
    return tiledb.Ctx(config)


def _to_tiledb_schema(schema: ZarrSchema):
    dims = [
        tiledb.Dim(
            name=dim.name,
            domain=dim.domain,
            dtype=dim.dtype,
            tile=dim.tile,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=7)]),
        )
        for dim in schema.domain.dims()
    ]
    attrs = [_to_tiledb_attr(attr) for attr in schema.attrs()]
    return tiledb.ArraySchema(
        domain=tiledb.Domain(*dims),
        attrs=attrs,
        offsets_filters=tiledb.FilterList([tiledb.PositiveDeltaFilter()]),
    )


def _to_tiledb_attr(attr):
    kwargs = {
        'name': attr.name,
        'dtype': attr.dtype,
        'filters': tiledb.FilterList([tiledb.ZstdFilter(level=7)]),
    }
    if attr.var:
        kwargs['var'] = True
    if attr.fill is not None:
        kwargs['fill'] = attr.fill
    if attr.nullable:
        kwargs['nullable'] = True
    return tiledb.Attr(**kwargs)
