import pytest
from typing import List
import tiledb

from silvimetric.commands import scan, info, shatter, manage
from silvimetric import ShatterConfig, Storage
from silvimetric.resources.config import StorageConfig

from test_shatter import confirm_one_entry


class TestCommands(object):
    def test_scan(
        self, shatter_config: ShatterConfig, storage_config: StorageConfig
    ):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10, 5)
        assert res['tile_info']['recommended'] == 1

        # recommended here should follow cell counts
        rec = 100 if storage_config.alignment == 'AlignToCorner' else 121
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, depth=5)
        assert res['tile_info']['recommended'] == rec

    def test_info(self, tdb_filepath: str, config_split: List[ShatterConfig]):
        i = info.info(tdb_filepath)
        assert bool(i['history'])
        assert len(i['history']) == 4

        for idx, c in enumerate(i['history']):
            osc = config_split[idx]
            assert c == osc.to_json()

        nb = next(iter(config_split[0].bounds.bisect()))
        i = info.info(tdb_filepath, bounds=nb)
        assert len(i['history']) == 1
        assert i['history'][0] == config_split[0].to_json()

        i = info.info(tdb_filepath, name=str(config_split[0].name))
        assert len(i['history']) == 1
        assert i['history'][0] == config_split[0].to_json()

        dates = config_split[1].date
        i = info.info(tdb_filepath, dates=dates)
        assert len(i['history']) == 1
        assert i['history'][0] == config_split[1].to_json()

    def test_delete(self, tdb_filepath: str, config_split: List[ShatterConfig]):
        # TODO shore up this test and functionality.
        # deletion is not perfect with TileDB dense arrays, and it may
        # take several rounds of consolidation for it to fully work.
        # To fix this, we may need to move timestamps to a dimension instead of
        # using the timetravel options

        ids = [c.name for c in config_split]

        s = Storage.from_db(tdb_filepath)
        for i, pid in enumerate(ids):
            manage.delete(tdb_filepath, pid)

            time_slot = i + 1
            h = info.info(tdb_filepath, name=pid)
            assert h['history']
            sc = ShatterConfig.from_dict(h['history'][0])
            assert sc.name == pid
            assert not sc.finished
            assert sc.time_slot == time_slot

            # not working 100% of the time at the moment
            with s.open(mode='r', timestamp=sc.timestamp) as reader:
                a = reader.df[:]
                assert a[a.shatter_process_num == time_slot].empty

        assert s.config.next_time_slot == 5

    def test_restart(
        self,
        tdb_filepath: str,
        config_split: List[ShatterConfig],
        maxy: float,
        test_point_count: int,
    ):
        ids = [c.name for c in config_split]
        s = Storage.from_db(tdb_filepath)
        for pid in ids:
            h1 = info.info(storage=s, name=pid)
            mbr1 = h1['history'][0]['mbr']

            manage.restart(tdb_filepath, pid)
            h2 = info.info(storage=s, name=pid)
            mbr2 = h2['history'][0]['mbr']

            assert mbr1 == mbr2
            assert h2['history'][0]['name'] == str(pid)
            assert h2['history'][0]['finished']

        base = 11 if s.config.alignment == 'AlignToCenter' else 10
        maxy = maxy + 15 if s.config.alignment == 'AlignToCenter' else maxy
        confirm_one_entry(s, maxy, base, test_point_count)

    def test_resume(
        self, shatter_config: ShatterConfig, storage_config: StorageConfig
    ):
        # if AlignToCorner, then the count is 3/4 of the total when
        #     mbr = ((0,4), (0,4)),)
        # if AlignToCenter, extents are shifted half a pixel to the right,
        #     static pc inserted here
        partial_count = (
            67500.0 if storage_config.alignment == 'AlignToCorner' else 86400
        )

        # modify shatter config
        shatter_config.tile_size = 1
        shatter_config.mbr = (((0, 4), (0, 4)),)
        # send to shatter
        pc = shatter.shatter(shatter_config)
        assert pc == partial_count
