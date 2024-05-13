import numpy as np
import os

from silvimetric.cli import cli
from silvimetric.commands import shatter, info
from silvimetric import ShatterConfig, Storage

class TestCli(object):
    def test_cli_init(self, tmp_path_factory, runner, bounds):
        path = tmp_path_factory.mktemp("test_tdb")
        p = os.path.abspath(path)
        res = runner.invoke(cli.cli, args=["-d", p, "--debug",
                "--scheduler", "single-threaded", "initialize", "--resolution",
                '10', '--crs', 'EPSG:3857', '--bounds', str(bounds)],
                catch_exceptions=False)
        assert res.exit_code == 0

    def test_cli_shatter(self, runner, maxy, date, tdb_filepath,
            copc_filepath, storage):

        res = runner.invoke(cli.cli, args=["-d", tdb_filepath,
                "--scheduler", "single-threaded",
                "shatter", copc_filepath,
                "--date", date.isoformat()+'Z',
                "--tilesize", '10'], catch_exceptions=False)
        assert res.exit_code == 0

        with storage.open('r') as a:
            assert a[:,:]['Z'].shape[0] == 100
            xdom = a.schema.domain.dim('X').domain[1]
            ydom = a.schema.domain.dim('Y').domain[1]
            assert xdom == 10
            assert ydom == 10

            for xi in range(xdom):
                for yi in range(ydom):
                    a[xi, yi]['Z'].size == 1
                    a[xi, yi]['Z'][0].size == 900
                    # this should have all indices from 0 to 9 filled.
                    # if oob error, it's not this test's fault
                    assert bool(np.all( a[xi, yi]['Z'][0] == ((maxy/storage.config.resolution) - (yi + 1)) ))

    def test_cli_scan(self, runner, copc_filepath, storage_config):
        tdb_dir = storage_config.tdb_dir
        res = runner.invoke(cli.cli, args=['-d', tdb_dir, '--scheduler', 'single-threaded', 'scan', copc_filepath])
        print(res.output)
        assert res.exit_code == 0

    def test_cli_info(self, tdb_filepath, runner, shatter_config):
        shatter.shatter(shatter_config)
        res = runner.invoke(cli.cli, args=['-d', tdb_filepath, '--scheduler', 'single-threaded', 'info'])
        assert res.exit_code == 0

    def test_cli_extract(self, runner, extract_config, storage):
        atts = []
        for a in extract_config.attrs:
            atts.append('-a')
            atts.append(a.name)
        ms = []
        for m in extract_config.metrics:
            ms.append('-m')
            ms.append(m.name)
        out_dir = extract_config.out_dir
        tdb_dir = extract_config.tdb_dir

        res = runner.invoke(cli.cli, args=['-d', tdb_dir, '--scheduler',
            'single-threaded', 'extract', *atts, *ms, '--outdir' ,out_dir])

        assert res.exit_code == 0

    def test_cli_delete(self, runner, shatter_config, pre_shatter):
        i = shatter_config.name
        res = runner.invoke(cli.cli, ['-d', shatter_config.tdb_dir,
                '--scheduler', 'single-threaded', 'delete', '--id', i])
        assert res.exit_code == 0

        i = info.info(shatter_config.tdb_dir)
        assert len(i['history']) == 0

    def test_cli_restart(self, runner, shatter_config, pre_shatter):

        i = shatter_config.name
        res = runner.invoke(cli.cli, ['-d', shatter_config.tdb_dir,
                '--scheduler', 'single-threaded', 'restart', '--id', i])
        assert res.exit_code == 0
        i = info.info(shatter_config.tdb_dir)
        assert len(i['history']) == 1
        assert ShatterConfig.from_dict(i['history'][0])

    def test_cli_resume(self, runner, shatter_config, pre_shatter):

        i = shatter_config.name
        res = runner.invoke(cli.cli, ['-d', shatter_config.tdb_dir,
                '--scheduler', 'single-threaded', 'restart', '--id', i])
        assert res.exit_code == 0
        i = info.info(shatter_config.tdb_dir)
        assert len(i['history']) == 1
        assert ShatterConfig.from_dict(i['history'][0])