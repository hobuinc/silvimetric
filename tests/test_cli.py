import pytest
import numpy as np

from silvimetric.cli import cli

class TestCli(object):
    def test_cli_init(self, tdb_filepath, runner, bounds):
        res = runner.invoke(cli.cli, args=["-d", tdb_filepath, "--debug",
                "initialize", "--resolution", '10', '--crs', 'EPSG:3857',
                '--bounds', str(bounds)], catch_exceptions=False)
        assert res.exit_code == 0
        assert f"Initializing SilviMetric Database at '{tdb_filepath}'" in res.output

    def test_cli_shatter(self, runner, storage, maxy, date, tdb_filepath,
            copc_filepath):

        res = runner.invoke(cli.cli, args=["-d", tdb_filepath,
                "--scheduler", "single-threaded",
                "shatter", copc_filepath,
                "--date", date.isoformat()+'Z',
                "--tilesize", '10'], catch_exceptions=False)
        assert res.exit_code == 0

        with storage.open('r') as a:
            y = a[:,0]['Z'].shape[0]
            x = a[0,:]['Z'].shape[0]
            assert y == 10
            assert x == 10
            for xi in range(x):
                for yi in range(y):
                    a[xi, yi]['Z'].size == 1
                    assert bool(np.all(a[xi, yi]['Z'][0] == ((maxy/storage.config.resolution)-yi)))