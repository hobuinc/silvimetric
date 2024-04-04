import uuid
import json
from time import sleep
from typing import List
from datetime import datetime

from silvimetric.commands import scan, info, shatter, manage
from silvimetric.resources import ShatterConfig, Storage

import pytest
import conftest

@pytest.fixture(scope='function')
def config_split(shatter_config: ShatterConfig) -> List[ShatterConfig]:
    sc = shatter_config
    config_split = []
    day = 1
    for b in sc.bounds.bisect():
        day += 1
        date = datetime(2011, 1, day)

        config_split.append(ShatterConfig(
            filename=sc.filename,
            date=date,
            attrs=sc.attrs,
            metrics=sc.metrics,
            bounds=b,
            name=uuid.uuid4(),
            tile_size=sc.tile_size,
            tdb_dir=sc.tdb_dir,
            log=sc.log
        ))

    for s in config_split:
        shatter.shatter(s)
    sleep(1)

    return config_split

class TestCommands(object):

    def test_scan(self, shatter_config):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10, 5)
        assert res == 1
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, depth=5)
        assert res == 100

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

        for i in range(1,len(ids)+1):
            h = info.info(tdb_dir=tdb_filepath)

            assert bool(h['history'])
            assert len(h['history']) == 5-i
            idx = i - 1
            manage.delete(tdb_filepath, ids[idx])

        h = info.info(tdb_dir=tdb_filepath)
        assert not bool(h['history'])

    def test_restart(self, tdb_filepath, config_split):
        ids = [c.name for c in config_split]

        for i in range(1,len(ids)+1):
            h = info.info(tdb_dir=tdb_filepath)

            assert bool(h['history'])
            assert len(h['history']) == 4
            idx = i - 1
            manage.restart(tdb_filepath, ids[idx])

        h = info.info(tdb_dir=tdb_filepath)
        assert bool(h['history'])
        assert len(h['history']) == 4

    def test_resume(self, shatter_config: ShatterConfig, test_point_count):
        # test that shatter only operates on cells not touched by nonempty_domain
        storage = Storage.from_db(shatter_config.tdb_dir)

        # modify shatter config
        shatter_config.tile_size=1
        shatter_config.mbr = (((0,4), (0,4)),)
        # send to shatter
        pc = shatter.shatter(shatter_config)
        assert pc == test_point_count - (test_point_count / 4)
        # check output config to see what the nonempthy_domain is
        #   - should be only only values outside of that tuple
        m = json.loads(storage.getMetadata('shatter', 1))
        assert len(m['mbr']) == 75
