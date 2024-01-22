import uuid
import json
from time import sleep
import numpy as np

from silvimetric.commands import scan, info, shatter
from silvimetric.resources import Bounds, ShatterConfig

import pytest
import conftest

@pytest.fixture(scope='class')
def config_split():
    pass

class TestCommands(object):

    def test_scan(self, shatter_config):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10)
        assert res == 25

    def test_info(self, shatter_config: ShatterConfig):

        sc = shatter_config
        tdb_dir = sc.tdb_dir
        i = info.info(tdb_dir)
        assert not bool(i['history'])

        sc_list = []
        date = np.datetime64('2011-01-01')
        for b in sc.bounds.bisect():
            date += 1
            sc_list.append(ShatterConfig(
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

        for s in sc_list:
            shatter.shatter(s)
        sleep(1)

        i = info.info(tdb_dir)
        assert bool(i['history'])
        assert len(i['history']) == 4

        for idx, c in enumerate(i['history']):
            nsc = ShatterConfig.from_string(json.dumps(c))
            osc = sc_list[idx]
            assert nsc.name == osc.name
            assert nsc.bounds == osc.bounds

        # bounds filtering
        nb = list(sc_list[0].bounds.bisect())[0]
        i = info.info(tdb_dir, bounds=nb)
        assert len(i['history']) == 1
        assert i['history'][0] == sc_list[0].to_json()

        # shatter process name filtering
        i = info.info(tdb_dir, name=str(sc_list[0].name))
        assert len(i['history']) == 1
        assert i['history'][0] == sc_list[0].to_json()

        # time filtering
        d = sc_list[1].date
        i = info.info(tdb_dir, start_time=d, end_time=d)
        assert len(i['history']) == 1
        assert i['history'][0] == sc_list[1].to_json()