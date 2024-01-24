import uuid
from time import sleep
from typing import List
from datetime import datetime

from silvimetric.commands import scan, info, shatter
from silvimetric.resources import ShatterConfig

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
            sc.filename,
            date,
            sc.attrs,
            sc.metrics,
            b,
            uuid.uuid4(),
            sc.tile_size,
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
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10)
        assert res == 25

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