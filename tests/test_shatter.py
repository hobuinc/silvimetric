import os
import json
import uuid
import datetime
from math import ceil

import pytest
import numpy as np
import dask
import tiledb

from silvimetric import Extents, Log, info, shatter, Storage
from silvimetric import ShatterConfig
from silvimetric.resources.attribute import Attribute
from silvimetric.resources.metric import Metric


@dask.delayed
def write(x, y, val, s: Storage, attrs, dims, metrics):
    m_list = [m.entry_name(a.name) for m in metrics for a in attrs]
    data = {
        a.name: np.array([np.array([val], dims[a.name]), None], object)[:-1]
        for a in attrs
    }

    for m in m_list:
        data[m] = [val]

    data['count'] = [val]
    data['start_datetime'] = [0]
    data['end_datetime'] = [datetime.datetime.now().timestamp()]
    with s.open('w') as w:
        w[x, y] = data


def confirm_one_entry(storage, maxy, base, pointcount, num_entries=1):
    xysize = base
    shape = xysize**2 * num_entries
    pc = pointcount * num_entries

    with storage.open('r') as a:
        assert a[:, :]['Z'].shape[0] == shape
        xdom = int(a.schema.domain.dim('X').domain[1])
        ydom = int(a.schema.domain.dim('Y').domain[1])
        assert xdom == xysize
        assert ydom == xysize
        assert a[:, :]['count'].sum() == pc
        val_const = ceil(maxy / storage.config.resolution)

        for xi in range(xdom):
            for yi in range(ydom):
                for i in range(a[xi, yi]['Z'].size):
                    # assert np.all(a[xi, yi]['Z'][i] >= 9)
                    # assert np.all(a[xi, yi]['Z'][i] <= 20)
                    assert bool(
                        np.all(a[xi, yi]['Z'][i] == (val_const - yi - 1))
                    )


class Test_Shatter(object):
    def test_command(
        self,
        shatter_config: ShatterConfig,
        storage: Storage,
        test_point_count: int,
        threaded_dask
    ):
        shatter(shatter_config)
        base = 11 if storage.config.alignment == 'AlignToCenter' else 10
        maxy = storage.config.root.maxy
        confirm_one_entry(storage, maxy, base, test_point_count)

    def test_multiple(
        self,
        shatter_config: ShatterConfig,
        storage: Storage,
        test_point_count: int,
        threaded_dask
    ):
        shatter(shatter_config)
        base = 11 if storage.config.alignment == 'AlignToCenter' else 10
        maxy = storage.config.root.maxy
        confirm_one_entry(storage, maxy, base, test_point_count)

        # change attributes to make it a new run
        shatter_config.name = uuid.uuid4()
        shatter_config.mbr = ()
        shatter_config.time_slot = 2
        shatter_config.date = (datetime.datetime(2009,1,1),datetime.datetime(2010,1,1))
        shatter(shatter_config)
        confirm_one_entry(storage, maxy, base, test_point_count, 2)

        # check that you can query results by datetime
        with storage.open('r') as a:
            a:tiledb.SparseArray
            query_time = datetime.datetime(2009,6,1).timestamp()
            q = a.query(cond=f'start_datetime >= {query_time}')
            assert len(q.df[:,:]['start_datetime']) == base ** 2

        m = info(storage.config.tdb_dir)
        assert len(m['history']) == 2

    def test_parallel(
        self,
        storage: Storage,
        attrs: list[Attribute],
        dims: dict,
        threaded_dask: None,
        metrics: list[Metric],
    ):
        # test that writing in parallel doesn't affect ordering of values
        # constrained by NumberOfReturns being uint8

        count = 255
        tl = [
            write(0, 0, val, storage, attrs, dims, metrics)
            for val in range(count)
        ]

        dask.compute(tl)

        with storage.open('r') as r:
            d = r[0, 0]
            for idx in range(count):
                assert bool(np.all(d['Z'][idx] == d['Intensity'][idx]))
                assert bool(
                    np.all(d['Intensity'][idx] == d['NumberOfReturns'][idx])
                )
                assert bool(
                    np.all(d['NumberOfReturns'][idx] == d['ReturnNumber'][idx])
                )

    def test_config(
        self,
        shatter_config: ShatterConfig,
        storage: Storage,
        test_point_count: int,
        threaded_dask
    ):
        shatter(shatter_config)
        try:
            meta = storage.getMetadata('shatter', shatter_config.time_slot)
        except BaseException as e:
            pytest.fail("Failed to retrieve 'shatter' metadata key." + e.args)
        meta_j = json.loads(meta)
        pc = meta_j['point_count']
        assert pc == test_point_count

    @pytest.mark.parametrize(
        'sh_cfg', ['shatter_config', 'uneven_shatter_config']
    )
    def test_sub_bounds(
        self,
        sh_cfg: str,
        test_point_count: int,
        request: pytest.FixtureRequest,
        maxy: float,
        alignment: str,
        threaded_dask
    ):
        s = request.getfixturevalue(sh_cfg)
        storage = Storage.from_db(s.tdb_dir)
        e = Extents.from_storage(s.tdb_dir)

        pc = 0
        for b in e.split():
            log = Log(20)
            time_slot = storage.reserve_time_slot()
            sc = ShatterConfig(
                tdb_dir=s.tdb_dir,
                log=log,
                filename=s.filename,
                tile_size=s.tile_size,
                bounds=b.bounds,
                date=s.date,
                time_slot=time_slot,
            )
            pc = pc + shatter(sc)
        history = info(s.tdb_dir)['history']
        assert len(history) == 4
        assert isinstance(history, list)
        pcs = [h['point_count'] for h in history]

        # When alignment is point and using uneven_shatter_config, the bounds
        # will be changed so that not all points are grabbed. This is expected.
        if alignment != 'AlignToCenter' or sh_cfg != 'uneven_shatter_config':
            assert sum(pcs) == test_point_count
            assert pc == test_point_count

        with storage.open('r') as a:
            data = a.query(attrs=['Z'], coords=True, use_arrow=False).df[:]
            data = data.set_index(['X', 'Y'])

            minx = int(data.reset_index().X.min())
            maxx = int(data.reset_index().X.max())
            miny = int(data.reset_index().Y.min())
            maxy = int(data.reset_index().Y.max())

            for xi in range(minx, maxx + 1):
                for yi in range(miny, maxy + 1):
                    curr = data.loc[xi, yi]
                    # check that each cell only has one allocation
                    assert curr.size == 1.0

    def test_partial_overlap(
        self, partial_shatter_config: ShatterConfig, alignment: int
    ):
        pc = shatter(partial_shatter_config)
        actual = 22500 if alignment == 'AlignToCorner' else 32400
        assert pc == actual

    @pytest.mark.skipif(
        os.environ.get('AWS_SECRET_ACCESS_KEY') is None
        or os.environ.get('AWS_ACCESS_KEY_ID') is None,
        reason='Missing necessary AWS environment variables',
    )
    def test_remote_creation(
        self,
        s3_shatter_config: ShatterConfig,
        s3_storage: Storage,
    ):
        # need processes scheduler to accurately test bug fix
        dask.config.set(scheduler='processes')
        maxy = s3_storage.config.root.maxy
        base = 11
        point_count = 108900
        shatter(s3_shatter_config)
        confirm_one_entry(s3_storage, maxy, base, point_count, 1)
