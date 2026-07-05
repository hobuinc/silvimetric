import numpy as np
import pandas as pd
import pytest
import os
import copy
import time

from datetime import datetime

from silvimetric import (
    Storage,
    all_metrics,
    Attribute,
    Attributes,
    StorageConfig,
)
from silvimetric import __version__ as svversion
from silvimetric.resources.config import ShatterConfig


def _backend_storage(
    tmp_path_factory,
    protocol,
    metrics,
    crs,
    resolution,
    alignment,
    attrs,
    bounds,
) -> Storage:
    path = tmp_path_factory.mktemp(f'test_{protocol}_equivalence')
    uri = f'{protocol}://{os.path.abspath(path)}'
    sc = StorageConfig(
        tdb_dir=uri,
        crs=crs,
        resolution=resolution,
        alignment=alignment,
        attrs=copy.deepcopy(attrs),
        metrics=copy.deepcopy(metrics),
        root=copy.deepcopy(bounds),
        xsize=5,
        ysize=5,
    )
    return Storage.create(sc)


def _write_records(storage: Storage, rows, dates) -> None:
    records = []
    attrs = storage.get_attributes()
    for row in rows:
        record = {
            'xi': row['xi'],
            'yi': row['yi'],
            'count': row['count'],
            'shatter_process_num': row['process'],
        }
        for idx, attr in enumerate(attrs):
            value = row['base'] + idx
            record[attr.name] = np.array(
                [value, value + 0.25],
                dtype=attr.dtype.subtype,
            )
        for idx, name in enumerate(storage.get_derived_names()):
            record[name] = float(row['base'] + idx)
        records.append(record)
    storage.write(pd.DataFrame(records), dates)


def _normal_value(value):
    if isinstance(value, np.ndarray):
        return tuple(value.tolist())
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _normalized_frame(storage: Storage, timestamp=None) -> pd.DataFrame:
    with storage.open('r', timestamp=timestamp) as array:
        df = array.df[:, :].copy()
    df = df.drop(columns=[c for c in df.columns if c.startswith('_')])
    df = df.sort_values(['X', 'Y']).reset_index(drop=True)
    for column in df.columns:
        df[column] = df[column].map(_normal_value)
    return df


def _assert_equal_content(left: pd.DataFrame, right: pd.DataFrame) -> None:
    pd.testing.assert_frame_equal(
        left,
        right[left.columns],
        check_dtype=False,
        check_exact=True,
    )


class Test_Storage(object):
    @pytest.mark.parametrize(
        ('protocol', 'backend'),
        [('zarr', 'zarr'), ('icechunk', 'zarr'), ('tiledb', 'tiledb')],
    )
    def test_backend_protocol_selection(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        alignment,
        attrs,
        bounds,
        protocol,
        backend,
    ):
        path = tmp_path_factory.mktemp(f'test_{protocol}')
        p = os.path.abspath(path)
        uri = f'{protocol}://{p}'
        sc = StorageConfig(
            tdb_dir=uri,
            crs=crs,
            resolution=resolution,
            alignment=alignment,
            attrs=attrs,
            metrics=metrics,
            root=bounds,
            xsize=5,
            ysize=5
        )

        storage = Storage.create(sc)
        assert storage.backend_name == backend
        assert storage.storage_uri == p

        reopened = Storage.from_db(uri)
        assert reopened.backend_name == backend
        assert reopened.storage_uri == p

    @pytest.mark.parametrize('protocol', ['zarr', 'tiledb'])
    def test_backend_write_roundtrip(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        alignment,
        attrs,
        bounds,
        date,
        protocol,
    ):
        path = tmp_path_factory.mktemp(f'test_{protocol}_write')
        uri = f'{protocol}://{os.path.abspath(path)}'
        sc = StorageConfig(
            tdb_dir=uri,
            crs=crs,
            resolution=resolution,
            alignment=alignment,
            attrs=attrs,
            metrics=metrics,
            root=bounds,
            xsize=5,
            ysize=5
        )
        storage = Storage.create(sc)
        record = {
            'xi': [0],
            'yi': [0],
            'count': [1],
            'shatter_process_num': [1],
        }
        for attr in attrs:
            record[attr.name] = [
                np.array([1], dtype=attr.dtype.subtype)
            ]
        for name in storage.get_derived_names():
            record[name] = [1.0]

        storage.write(pd.DataFrame(record), date)

        with storage.open('r') as array:
            data = array.df[:, :]
            assert len(data) == 1
            assert data['count'].iloc[0] == 1
            assert data['shatter_process_num'].iloc[0] == 1

    def test_zarr_and_tiledb_write_identical_content(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        alignment,
        attrs,
        bounds,
    ):
        zarr_storage = _backend_storage(
            tmp_path_factory,
            'zarr',
            metrics,
            crs,
            resolution,
            alignment,
            attrs,
            bounds,
        )
        tiledb_storage = _backend_storage(
            tmp_path_factory,
            'tiledb',
            metrics,
            crs,
            resolution,
            alignment,
            attrs,
            bounds,
        )
        dates = (datetime(2021, 1, 1), datetime(2021, 12, 31))
        rows = [
            {'xi': 0, 'yi': 0, 'count': 3, 'process': 1, 'base': 10},
            {'xi': 1, 'yi': 0, 'count': 4, 'process': 1, 'base': 20},
        ]

        _write_records(zarr_storage, rows, dates)
        _write_records(tiledb_storage, rows, dates)

        _assert_equal_content(
            _normalized_frame(zarr_storage),
            _normalized_frame(tiledb_storage),
        )

    def test_zarr_and_tiledb_time_slicing_identical_content(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        alignment,
        attrs,
        bounds,
    ):
        zarr_storage = _backend_storage(
            tmp_path_factory,
            'zarr',
            metrics,
            crs,
            resolution,
            alignment,
            attrs,
            bounds,
        )
        tiledb_storage = _backend_storage(
            tmp_path_factory,
            'tiledb',
            metrics,
            crs,
            resolution,
            alignment,
            attrs,
            bounds,
        )
        first_dates = (datetime(2021, 1, 1), datetime(2021, 6, 1))
        second_dates = (datetime(2022, 1, 1), datetime(2022, 6, 1))
        first_rows = [
            {'xi': 0, 'yi': 0, 'count': 3, 'process': 1, 'base': 10},
            {'xi': 1, 'yi': 0, 'count': 4, 'process': 1, 'base': 20},
        ]
        second_rows = [
            {'xi': 0, 'yi': 0, 'count': 7, 'process': 2, 'base': 30},
            {'xi': 1, 'yi': 0, 'count': 8, 'process': 2, 'base': 40},
        ]

        _write_records(zarr_storage, first_rows, first_dates)
        zarr_first_ts = int(time.time() * 1000)
        time.sleep(0.01)
        _write_records(zarr_storage, second_rows, second_dates)
        zarr_second_ts = int(time.time() * 1000)

        _write_records(tiledb_storage, first_rows, first_dates)
        tiledb_first_ts = int(time.time() * 1000)
        time.sleep(0.01)
        _write_records(tiledb_storage, second_rows, second_dates)
        tiledb_second_ts = int(time.time() * 1000)

        first_frames = [
            _normalized_frame(zarr_storage, timestamp=(0, zarr_first_ts)),
            _normalized_frame(tiledb_storage, timestamp=(0, tiledb_first_ts)),
        ]
        second_frames = [
            _normalized_frame(zarr_storage, timestamp=(0, zarr_second_ts)),
            _normalized_frame(tiledb_storage, timestamp=(0, tiledb_second_ts)),
        ]

        _assert_equal_content(first_frames[0], first_frames[1])
        _assert_equal_content(second_frames[0], second_frames[1])

        for frame in first_frames:
            assert frame['shatter_process_num'].tolist() == [1, 1]
            assert frame['count'].tolist() == [3, 4]
        for frame in second_frames:
            assert frame['shatter_process_num'].tolist() == [2, 2]
            assert frame['count'].tolist() == [7, 8]

    def test_schema(self, storage: Storage, attrs: list[Attribute]):
        with storage.open('r') as st:
            s = st.schema
            assert s.has_attr('count')
            assert s.attr('count').dtype == np.uint32

            for a in attrs:
                assert s.has_attr(a.name)
                # assert s.attr(a.name) == a.schema()

    def test_time_reserve(self, storage):
        for x in range(5):
            time_slot = storage.reserve_time_slot()
            assert time_slot == x + 1

    def test_local(self, storage: Storage, attrs: list[Attribute]):
        with storage.open('r') as st:
            sc = st.schema
            assert sc.has_attr('shatter_process_num')
            assert sc.attr('shatter_process_num').dtype == np.uint16
            assert sc.has_attr('count')
            assert sc.attr('count').dtype == np.uint32

            for a in attrs:
                assert sc.has_attr(a.name)
                # assert sc.attr(a.name) == a.schema()

    def test_config(self, storage: Storage):
        """Check that instantiation metadata is properly written"""

        storage.save_config()
        config = storage.get_config()
        assert config.resolution == storage.config.resolution
        assert config.alignment == storage.config.alignment
        assert config.root == storage.config.root
        assert config.crs == storage.config.crs
        assert storage.config.version == svversion

    def test_metric_dependencies(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        alignment,
        attrs,
        bounds,
    ):
        ms = copy.deepcopy(metrics)

        path = tmp_path_factory.mktemp('test_tdb')
        p = os.path.abspath(path)

        ms[0].dependencies = [Attributes['HeightAboveGround']]
        sc = StorageConfig(
            tdb_dir=p,
            crs=crs,
            resolution=resolution,
            alignment=alignment,
            attrs=attrs,
            metrics=ms,
            root=bounds,
            xsize=5,
            ysize=5
        )

        with pytest.raises(ValueError) as e:
            Storage.create(sc)
        assert str(e.value) == 'Missing required dependency, HeightAboveGround.'

        ms[0].dependencies = [Attributes['NumberOfReturns']]
        s = Storage.create(sc)
        assert isinstance(s, Storage)

        ms[0].dependencies = []

    def test_oob_storage(
        self,
        tmp_path_factory,
        metrics,
        crs,
        resolution,
        attrs,
        bounds,
    ):
        ms = copy.deepcopy(metrics)
        path = tmp_path_factory.mktemp('test_tdb')
        p = os.path.abspath(path)

        sc = StorageConfig(
            tdb_dir=p,
            crs=crs,
            resolution=resolution,
            attrs=attrs,
            metrics=ms,
            root=bounds,
            xsize=1000,
            ysize=1000
        )

        s = Storage.create(sc)
        a = s.open('r')

        assert a.schema.domain.dim('X').tile == 12
        assert s.config.xsize == 12
        assert a.schema.domain.dim('Y').tile == 12
        assert s.config.ysize == 12


    def test_metrics(self, storage: Storage):
        m_list = storage.get_metrics()
        a_list = storage.get_attributes()

        with storage.open('r') as st:
            s = st.schema
            for m in m_list:
                assert m.name in all_metrics.keys()

                def e_name(att, met):
                    return s.attr(met.entry_name(att.name))

                def schema(att, met):
                    return all_metrics[met.name].schema(att)

                def attr_tuple(attr):
                    return (
                        attr.name,
                        np.dtype(attr.dtype),
                        getattr(attr, 'var', False),
                        getattr(attr, 'nullable', None)
                        if hasattr(attr, 'nullable')
                        else attr.isnullable,
                    )

                assert all(
                    [
                        attr_tuple(e_name(a, m)) == attr_tuple(schema(a, m))
                        for a in a_list
                    ]
                )

    def test_metadata(self, storage: Storage, shatter_config: ShatterConfig):
        shatter_config.time_slot = storage.reserve_time_slot()
        storage.save_shatter_meta(shatter_config)
        shc_copy = copy.deepcopy(shatter_config)
        shc_copy.time_slot = storage.reserve_time_slot()
        storage.save_shatter_meta(shc_copy)

        sh_c = storage.get_shatter_meta(shatter_config.time_slot)
        assert sh_c == shatter_config
        sh_c_2 = storage.get_shatter_meta(shc_copy.time_slot)
        assert sh_c_2 == shc_copy

        history = storage.get_history()
        assert len(history) == 2

        assert ShatterConfig.from_json(history[0])== shatter_config
        assert ShatterConfig.from_json(history[1]) == shc_copy
