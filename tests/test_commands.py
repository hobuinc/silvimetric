import uuid

from silvimetric.commands import scan, info, shatter
from silvimetric.resources import Bounds

import conftest

class TestCommands(object):

    def test_scan(self, shatter_config):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10)
        assert res == 25

    def test_info(self, shatter_config):
        tdb_dir = shatter_config.tdb_dir
        i = info.info(tdb_dir)
        assert not bool(i['history'])

        shatter.shatter(shatter_config)
        i = info.info(tdb_dir)
        assert bool(i['history'])

        shatter_config.name = uuid.uuid4()
        shatter(shatter_config)

        # TODO bounds filtering
        b: Bounds = shatter_config.bounds
        b1 = b.bisect()[0]
        i = info.info(tdb_dir, bounds=b1)


        # TODO shatter process name filtering
        # TODO time filtering
        print(i)