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
@click.option("--database", '-d', type=click.Path(exists=False), help="Database path")
@click.option("--debug", is_flag=True, default=False, help="Print debug messages?")
@click.option("--log-level", default="INFO", help="Log level (INFO/DEBUG)")
@click.option("--log-dir", default=None, help="Directory for log output", type=str)
@click.option("--progress", default=True, type=bool, help="Report progress")
@click.option("--workers", type=int, default=12, help="Number of workers for Dask")
@click.option("--threads", type=int, default=4, help="Number of threads per worker for Dask")
@click.option("--watch", is_flag=True, default=False, type=bool,
        help="Open dask diagnostic page in default web browser.")
@click.option("--dasktype", default='processes', type=click.Choice(['threads',
        'processes']), help="What Dask uses for parallelization. For more"
        "information see here https://docs.dask.org/en/stable/scheduling.html#local-threads")
@click.option("--scheduler", default='distributed', type=click.Choice(['distributed',
        'local', 'single-threaded']), help="Type of dask scheduler. Both are "
        "local, but are run with different dask libraries. See more here "
        "https://docs.dask.org/en/stable/scheduling.html.")
@click.pass_context
def cli(ctx, database, debug, log_level, log_dir, progress, dasktype, scheduler,
        workers, threads, watch):

    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    log = Log(log_level, log_dir)
    app = ApplicationConfig(tdb_dir=database,
            log=log,
            debug=debug,
            progress=progress,
            scheduler=scheduler)
    dask_handle(dasktype, scheduler, workers, threads, watch, app.log)
    ctx.obj = app


@cli.command("info")
@click.option("--bounds", type=BoundsParamType(), default=None,
        help="Bounds to filter by")
@click.option("--date", type=click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        help="Select processes with this date")
@click.option("--dates", type=click.Tuple([
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ'])]), nargs=2,
        help="Select processes within this date range")
@click.option("--name", type=str, default=None,
        help="Select processes with this name")
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
@click.option("--resolution", type=float, default=100,
        help="Summary pixel resolution")
@click.option("--filter", is_flag=True, type=bool, default=False,
        help="Remove empty space in computation. Will take extra time.")
@click.option("--point_count", type=int, default=600000,
        help="Point count threshold.")
@click.option("--depth", type=int, default=6,
        help="Quadtree depth threshold.")
@click.option("--bounds", type=BoundsParamType(), default=None,
        help="Bounds to scan.")
@click.pass_obj
def scan_cmd(app, resolution, point_count, pointcloud, bounds, depth, filter):
    """Scan point cloud and determine the optimal tile size."""
    return scan.scan(app.tdb_dir, pointcloud, bounds, point_count, resolution,
            depth, filter)


@cli.command('initialize')
@click.option("--bounds", type=BoundsParamType(), required=True,
        help="Root bounds that encapsulates all data")
@click.option("--crs", type=CRSParamType(), required=True,
        help="Coordinate system of data")
@click.option("--attributes", "-a", multiple=True, type=AttrParamType(),
        help="List of attributes to include in Database")
@click.option("--metrics", "-m", multiple=True, type=MetricParamType(),
        help="List of metrics to include in Database")
@click.option("--resolution", type=float, default=30.0,
        help="Summary pixel resolution")
@click.pass_obj
def initialize_cmd(app: ApplicationConfig, bounds: Bounds, crs: pyproj.CRS,
        attributes: list[Attribute], resolution: float, metrics: list[Metric]):
    import itertools
    """Initialize silvimetrics DATABASE
    """
    storageconfig = StorageConfig(tdb_dir = app.tdb_dir,
            log = app.log,
            root = bounds,
            crs = crs,
            attrs = attributes,
            metrics = list(itertools.chain(*metrics)),
            resolution = resolution)
    return initialize.initialize(storageconfig)


@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--bounds", type=BoundsParamType(), default=None,
        help="Bounds for data to include in processing")
@click.option("--tilesize", type=int, default=None,
        help="Number of cells to include per tile")
@click.option("--report", is_flag=True, default=False, type=bool,
        help="Whether or not to write a report of the process, useful for debugging")
@click.option("--date", type=click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        help="Date the data was produced.")
@click.option("--dates", type=click.Tuple([
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ']),
        click.DateTime(['%Y-%m-%d','%Y-%m-%dT%H:%M:%SZ'])]), nargs=2,
        help="Date range the data was produced during")
@click.pass_obj
def shatter_cmd(app, pointcloud, bounds, report, tilesize, date, dates):

    if date is not None and dates is not None:
        app.log.warning("Both 'date' and 'dates' specified. Prioritizing 'dates'")

    if date is None and dates is None:
        raise ValueError("One of '--date' or '--dates' must be provided.")

    """Insert data provided by POINTCLOUD into the silvimetric DATABASE"""
    config = ShatterConfig(tdb_dir = app.tdb_dir,
            date=dates if dates else tuple([date]),
            log = app.log,
            filename = pointcloud,
            bounds = bounds,
            tile_size=tilesize)

    if report:
        if app.scheduler != 'distributed':
            app.log.warning('Report option is incompatible with scheduler'
                            '{scheduler}, skipping.')
        report_path = f'reports/{config.name}.html'
        with performance_report(report_path) as pr:
            shatter.shatter(config)
        print(f'Writing report to {report_path}.')
    else:
        shatter.shatter(config)


@cli.command('extract')
@click.option("--attributes", "-a", multiple=True, type=AttrParamType(),
        help="List of attributes to include output")
@click.option("--metrics", "-m", multiple=True, type=MetricParamType(),
        help="List of metrics to include in output")
@click.option("--bounds", type=BoundsParamType(), default=None,
        help="Bounds for data to include in output")
@click.option("--outdir", "-o", type=click.Path(exists=False), required=True,
        help="Output directory.")
@click.pass_obj
def extract_cmd(app, attributes, metrics, outdir, bounds):
    """Extract silvimetric metrics from DATABASE """

    #TODO only allow metrics and attributes to be added if they're present
    # in the storage config.

    config = ExtractConfig(tdb_dir = app.tdb_dir,
            log = app.log,
            out_dir= outdir,
            attrs = attributes,
            metrics = metrics,
            bounds = bounds)
    extract.extract(config)


if __name__ == "__main__":
    cli()
