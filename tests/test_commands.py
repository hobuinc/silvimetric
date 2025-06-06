import json
from typing import List

from silvimetric.commands import scan, info, shatter, manage
from silvimetric import ShatterConfig, Storage
from silvimetric.resources.config import StorageConfig


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

        d = config_split[1].date
        i = info.info(tdb_filepath, start_time=d, end_time=d)
        assert len(i['history']) == 1
        assert i['history'][0] == config_split[1].to_json()

    def test_delete(self, tdb_filepath: str, config_split: List[ShatterConfig]):
        ids = [c.name for c in config_split]

        for i in range(1, len(ids) + 1):
            h = info.info(tdb_dir=tdb_filepath)

            assert bool(h['history'])
            assert len(h['history']) == 5 - i
            idx = i - 1
            manage.delete(tdb_filepath, ids[idx])

        s = Storage.from_db(tdb_filepath)
        assert s.config.next_time_slot == 5

        h = info.info(tdb_dir=tdb_filepath)
        assert len(h['history']) == 0

    # TODO: re-add this once we figure out how to run certain tests serially
    # def test_311_failure(self, shatter_config, dask_proc_client):
    #     # make sure we handle tiledb contexts correctly within dask

    #     tdb_dir = shatter_config.tdb_dir
    #     shatter.shatter(shatter_config)

    #     i = info.info(tdb_dir)
    #     history = i['history'][-1]

    #     finished_config = ShatterConfig.from_dict(history)
    #     sh_id = finished_config.name
    #     manage.delete(tdb_dir, sh_id)

    def test_restart(
        self, tdb_filepath: str, config_split: List[ShatterConfig]
    ):
        ids = [c.name for c in config_split]

        for i in range(1, len(ids) + 1):
            h = info.info(tdb_dir=tdb_filepath)

            assert bool(h['history'])
            assert len(h['history']) == len(ids)
            manage.restart(tdb_filepath, ids[i - 1])

        h = info.info(tdb_dir=tdb_filepath)
        assert bool(h['history'])
        assert len(h['history']) == 4

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
