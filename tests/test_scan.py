from silvimetric.commands import scan

import conftest

class TestScan(object):

    def test_scan(self, shatter_config):
        s = shatter_config
        res = scan.scan(s.tdb_dir, s.filename, s.bounds, 10, 10)
        assert res == 25
