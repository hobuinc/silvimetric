import pytest
import json

from silvimetric.commands import scan, info, shatter, manage
from silvimetric import ShatterConfig, Storage


class TestCommands(object):
    def test_scan(self, shatter_config):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10, 5)
        assert res['tile_info']['recommended'] == 1

        res = scan.scan(s.tdb_dir, s.filename, s.bounds, depth=5)
        assert res['tile_info']['recommended'] == 100

    def test_info(self, tdb_filepath, config_split):
        i = info.info(tdb_filepath)
        assert bool(i['history'])
        assert len(i['history']) == 4

        for idx, c in enumerate(i['history']):
            osc = config_split[idx]
            assert c == osc.to_json()

        nb = list(config_split[0].bounds.bisect())[0]
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

    def test_delete(self, tdb_filepath, config_split):
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

    def test_311_failure(self, shatter_config, dask_proc_client):
        # make sure we handle tiledb contexts correctly within dask

        tdb_dir = shatter_config.tdb_dir
        shatter.shatter(shatter_config)

        i = info.info(tdb_dir)
        history = i['history'][-1]

        finished_config = ShatterConfig.from_dict(history)
        sh_id = finished_config.name
        manage.delete(tdb_dir, sh_id)

    def test_restart(self, tdb_filepath, config_split):
        ids = [c.name for c in config_split]

        for i in range(1, len(ids) + 1):
            h = info.info(tdb_dir=tdb_filepath)

            assert bool(h['history'])
            assert len(h['history']) == len(ids)
            manage.restart(tdb_filepath, ids[i - 1])

        h = info.info(tdb_dir=tdb_filepath)
        assert bool(h['history'])
        assert len(h['history']) == 4

    def test_resume(self, shatter_config: ShatterConfig, test_point_count):
        # test that shatter only operates on cells not touched by nonempty_domain
        storage = Storage.from_db(shatter_config.tdb_dir)

        # modify shatter config
        shatter_config.tile_size = 1
        shatter_config.mbr = (((0, 4), (0, 4)),)
        # send to shatter
        pc = shatter.shatter(shatter_config)
        assert pc == test_point_count - (test_point_count / 4)
        # check output config to see what the nonempthy_domain is
        #   - should be only only values outside of that tuple
        m = json.loads(storage.getMetadata('shatter', 1))
        assert len(m['mbr']) == 75
