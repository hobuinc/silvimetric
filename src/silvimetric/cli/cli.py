import click
from dask.distributed import performance_report
import pyproj
import logging

from ..resources import Bounds, Log
from ..resources import Attribute, Metric
from ..resources import StorageConfig, ShatterConfig, ExtractConfig, ApplicationConfig
from ..commands import shatter, extract, scan, info, initialize
from .common import BoundsParamType, CRSParamType, AttrParamType, MetricParamType
from .common import dask_handle

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
@click.option("--bounds", type=BoundsParamType(), default=None)
@click.option("--date", type=click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']))
@click.option("--dates", type=click.Tuple([
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ'])]), nargs=2)
@click.option("--name", type=str, default=None)
@click.pass_obj
def info_cmd(app, bounds, date, dates, name):
    import json
    if date is not None and dates is not None:
        app.log.warning("Both 'date' and 'dates' specified. Prioritizing 'dates'")

    start_date = dates[0] if dates else date
    end_date = dates[1] if dates else date

    i = info.info(app.tdb_dir, bounds=bounds, start_time=start_date,
        end_time=end_date, name=name)
    app.log.info(json.dumps(i, indent=2))


@cli.command("scan")
@click.argument("pointcloud", type=str)
@click.option("--resolution", type=float, default=100)
@click.option("--point_count", type=int, default=600000)
@click.option("--depth", type=int, default=6)
@click.option("--bounds", type=BoundsParamType(), default=None)
@click.option("--workers", type=int, default=12)
@click.option("--threads", type=int, default=4)
@click.option("--watch", is_flag=True, default=False, type=bool)
@click.option("--dasktype", default='cluster', type=click.Choice(['cluster',
              'threads', 'processes', 'single-threaded']))
@click.pass_obj
def scan_cmd(app, resolution, point_count, pointcloud, bounds, dasktype, workers,
             depth, threads, watch):
    """Scan point cloud and determine the optimal tile size."""
    dask_handle(dasktype, workers, threads, watch)
    return scan.scan(app.tdb_dir, pointcloud, bounds, point_count, resolution, depth)


@cli.command('initialize')
@click.option("--bounds", type=BoundsParamType(), required=True)
@click.option("--crs", type=CRSParamType(), required=True)
@click.option("--attributes", "-a", multiple=True, type=AttrParamType(),
              help="List of attributes to include in Database")
@click.option("--metrics", "-m", multiple=True, type=MetricParamType(),
              help="List of metrics to include in Database")
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.pass_obj
def initialize_cmd(app: ApplicationConfig, bounds: Bounds, crs: pyproj.CRS,
               attributes: list[Attribute], resolution: float, metrics: list[Metric]):
    """Initialize silvimetrics DATABASE
    """
    storageconfig = StorageConfig(tdb_dir = app.tdb_dir,
                                  log = app.log,
                                  root = bounds,
                                  crs = crs,
                                  attrs = attributes,
                                  metrics = metrics,
                                  resolution = resolution)
    return initialize.initialize(storageconfig)


@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--workers", type=int, default=12)
@click.option("--threads", type=int, default=4)
@click.option("--bounds", type=BoundsParamType(), default=None)
@click.option("--tilesize", type=int, default=None)
@click.option("--watch", is_flag=True, default=False, type=bool)
@click.option("--report", is_flag=True, default=False, type=bool)
@click.option("--date", type=click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']))
@click.option("--dates", type=click.Tuple([
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ'])]), nargs=2)
@click.option("--dasktype", default='cluster', type=click.Choice(['cluster',
              'threads', 'processes', 'single-threaded']))
@click.pass_obj
def shatter_cmd(app, pointcloud, workers, bounds, threads, watch, report,
                dasktype, tilesize, date, dates):
    if date is not None and dates is not None:
        app.log.warning("Both 'date' and 'dates' specified. Prioritizing 'dates'")
    """Insert data provided by POINTCLOUD into the silvimetric DATABASE"""
    dask_handle(dasktype, workers, threads, watch)
    config = ShatterConfig(tdb_dir = app.tdb_dir,
                            date=dates if dates else tuple([date]),
                            log = app.log,
                            filename = pointcloud,
                            bounds = bounds,
                            tile_size=tilesize)

    if report:
        if dasktype != 'cluster':
            app.log.warning('Report option is incompatible with dask type'
                            '{dasktype}, skipping.')
        report_path = f'reports/{config.name}.html'
        with performance_report(report_path) as pr:
            shatter.shatter(config)
        print(f'Writing report to {report_path}.')
    else:
        shatter.shatter(config)


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
    extract.extract(config)


if __name__ == "__main__":
    cli()
