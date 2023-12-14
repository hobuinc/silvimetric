import click
from dask.distributed import Client
import dask
import webbrowser
import pyproj

import json

import logging

from silvimetric.resources import Storage, Bounds, Log
from silvimetric.resources import Attributes, Metrics, Attribute, Metric
from silvimetric.resources import StorageConfig, ShatterConfig, ExtractConfig, ApplicationConfig
from silvimetric.commands import shatter, extract

@click.group()
@click.argument("database", type=click.Path(exists=False))
@click.option("--debug", is_flag=True, default=False, help="Print debug messages?")
@click.option("--log-level", default="INFO", help="Log level (INFO/DEBUG)")
@click.option("--log-dir", default=None, help="Directory for log output", type=str)
@click.option("--progress", default=True, type=bool, help="Report progress")
@click.pass_context
def cli(ctx, database, debug, log_level, log_dir, progress):

    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    log = Log(log_level, log_dir)
    app = ApplicationConfig(tdb_dir = database,
                            log = log,
                            debug = debug,
                            progress = progress)
    ctx.obj = app

@cli.command("info")
@click.option("--history", is_flag=True, default=False, type=bool)
@click.pass_obj
def info(app, history):
    """Print info about Silvimetric database"""
    with Storage.from_db(app.tdb_dir) as tdb:

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        # We don't have this until stuff has been put into the database
        try:
            shatter = json.loads(tdb.getMetadata('shatter'))

            # I don't know what this is?
        except KeyError:
            shatter = {}

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json(),
            'shatter': shatter
        }
        if history:
            try:
                # I don't know what this is? – hobu
                history = tdb.get_history()['shatter']
                if isinstance(history, list):
                    history = [ json.loads(h) for h in history ]
                else:
                    history = json.loads(history)
                info ['history'] = history
            except KeyError:
                history = {}
        print(json.dumps(info, indent=2))

class BoundsParamType(click.ParamType):
    name = "Bounds"

    def convert(self, value, param, ctx):
        try:
            b = Bounds.from_string(value)
            return b
        except ValueError:
            self.fail(f"{value!r} is not a bounds type", param, ctx)

class CRSParamType(click.ParamType):
    name = "CRS"

    def convert(self, value, param, ctx):
        try:
            crs = pyproj.CRS.from_user_input(value)
            return crs
        except Exception as e:
            self.fail(f"{value!r} is not a CRS type with error {e}", param, ctx)

class AttrParamType(click.ParamType):
    name="Attrs"
    def convert(self, value, param, ctx) -> list[Attribute]:
        try:
            return [Attributes[a] for a in value]
        except Exception as e:
            self.fail(f"{value!r} is not available in Attributes, {e}", param, ctx)

class MetricParamType(click.ParamType):
    name="Metrics"
    def convert(self, value, param, ctx) -> list[Metric]:
        try:
            return [Metrics[m] for m in value]
        except Exception as e:
            self.fail(f"{value!r} is not available in Metrics, {e}", param, ctx)

@cli.command('initialize')
@click.argument("bounds", type=BoundsParamType())
@click.argument("crs", type=CRSParamType())
@click.option("--attributes", "-a", multiple=True, type=AttrParamType(),
              help="List of attributes to include in Database")
@click.option("--metrics", "-m", multiple=True, type=MetricParamType(),
              help="List of metrics to include in Database")
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.pass_obj
def initialize(app: ApplicationConfig, bounds: Bounds, crs: pyproj.CRS,
               attributes: list[Attribute], resolution: float, metrics: list[Metric]):
    """Initialize silvimetrics DATABASE
    """
    from silvimetric.cli.initialize import initialize as initializeFunction
    storageconfig = StorageConfig(tdb_dir = app.tdb_dir,
                                  log = app.log,
                                  bounds = bounds,
                                  crs = crs,
                                  attrs = attributes,
                                  metrics = metrics,
                                  resolution = resolution)
    storage = initializeFunction(storageconfig)

@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--workers", type=int, default=12)
@click.option("--bounds", type=BoundsParamType(), default=None)
@click.option("--tilesize", type=int, default=16)
@click.option("--threads", default=4, type=int)
@click.option("--watch", is_flag=True, default=False, type=bool)
@click.option("--dasktype", default='cluster',
              type=click.Choice(['cluster', 'threads', 'processes',
                                 'single-threaded']))
@click.pass_obj
def shatter_cmd(app, pointcloud, workers, tilesize, threads, watch, bounds, dasktype):
    """Insert data provided by POINTCLOUD into the silvimetric DATABASE"""
    config = ShatterConfig(tdb_dir = app.tdb_dir,
                            log = app.log,
                            filename = pointcloud,
                            tile_size = tilesize,
                            bounds = bounds)

    if dasktype == 'cluster':
        with Client(n_workers=workers, threads_per_worker=threads) as client:
            if watch:
                webbrowser.open(client.cluster.dashboard_link)
            shatter(config, client)
    else:
        dask.config.set(scheduler=dasktype)
        shatter(config)


@cli.command('extract')
@click.option("--attributes", "-a", multiple=True, type=AttrParamType(),
              help="List of attributes to include in Database")
@click.option("--metrics", "-m", multiple=True, type=MetricParamType(),
              help="List of metrics to include in Database")
@click.option("--bounds", type=BoundsParamType(), default=None)
@click.option("--outdir", "-o", type=str, required=True)
@click.pass_obj
def extract_cmd(app, attributes, metrics, outdir, bounds):
    """Extract silvimetric metrics from DATABASE """

    config = ExtractConfig(tdb_dir = app.tdb_dir,
                           log = app.log,
                           out_dir= outdir,
                           attrs = attributes,
                           metrics = metrics,
                           bounds = bounds)
    extract(config)


if __name__ == "__main__":
    cli()
