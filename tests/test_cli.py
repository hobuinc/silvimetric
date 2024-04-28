import numpy as np

from silvimetric.cli import cli
from silvimetric.commands import shatter, info
from silvimetric import ShatterConfig

class TestCli(object):
    def test_cli_init(self, tdb_filepath, runner, bounds):
        res = runner.invoke(cli.cli, args=["-d", tdb_filepath, "--debug",
                "--scheduler", "single-threaded", "initialize", "--resolution",
                '10', '--crs', 'EPSG:3857', '--bounds', str(bounds)],
                catch_exceptions=False)
        assert res.exit_code == 0

    def test_cli_shatter(self, runner, storage, miny, date, tdb_filepath,
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
                    assert bool(np.all( a[xi, yi]['Z'][0] == ((miny/storage.config.resolution) + yi) ))

    def test_cli_scan(self, tdb_filepath, runner, copc_filepath, storage):
        res = runner.invoke(cli.cli, args=['-d', tdb_filepath, '--scheduler', 'single-threaded', 'scan', copc_filepath])
        assert res.exit_code == 0

    def test_cli_info(self, tdb_filepath, runner, shatter_config):
        shatter.shatter(shatter_config)
        res = runner.invoke(cli.cli, args=['-d', tdb_filepath, '--scheduler', 'single-threaded', 'info'])
        assert res.exit_code == 0

    def test_cli_extract(self, runner, extract_config):
        atts = ' '.join([f'-a {a.name}' for a in extract_config.attrs])
        ms = ' '.join([f'-m {m.name}' for m in extract_config.metrics])
        out_dir = extract_config.out_dir
        tdb_dir = extract_config.tdb_dir

        res = runner.invoke(cli.cli, args=(f'-d {tdb_dir} --scheduler '+
            f'single-threaded extract {atts} {ms} --outdir {out_dir}'))

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