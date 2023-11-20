import click
import logging
import json
import dask
from dask.distributed import Client
import webbrowser
import pyproj

from silvimetric.app import Application
from silvimetric.storage import Storage, Configuration
from silvimetric.shatter import shatter
from silvimetric.extract import extract
from silvimetric.bounds import Bounds

logger = logging.getLogger(__name__)

@click.group()
@click.argument("database", type=click.Path(exists=False))
@click.option("--log-level", default="INFO", help="Log level (INFO/DEBUG)")
@click.option("--progress", default=True, type=bool, help="Report progress")
@click.pass_context
def cli(ctx, database, log_level, progress):

    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level)

    # Log program args

    app = Application(
        log_level=log_level,
        progress = progress,
        tdb_dir = database,
    )
    ctx.obj = app


@cli.command("info")
@click.pass_obj
def info(app):
    """Print info about Silvimetric database"""
    with Storage(app.database) as tdb:
        meta = tdb.getMetadata()
        atts = tdb.getAttributes()
        info = { 'attributes': atts, 'metadata': meta }
        print(json.dumps(info, indent=2))

class BoundsParamType(click.ParamType):
    name = "Bounds"

    def convert(self, value, param, ctx):
        try:
            b = Bounds.loads(value)
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

@cli.command('initialize')
@click.argument("bounds", type=BoundsParamType())
@click.argument("crs", type=pyproj.CRS)
@click.option("--attributes", "-a", multiple=True,
              help="List of attributes to include in Database")
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.pass_obj
def initialize(app: Application, resolution: float, bounds: str, attributes: str, crs: str):
    """Initialize silvimetrics DATABASE
    
    
    
    """


    # :param resolution: Resolution of a cell in dataset units, default to 30.0
    # :type resolution: float, optional
    # """

    print(f"Creating database at {app.tdb_dir}")
    from silvimetric.cli.initialize import initialize as initializeFunction
    config = Configuration(app.tdb_dir, resolution, bounds, attributes, crs)
    initializeFunction(config)

@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--workers", type=int, default=12)
@click.option("--tilesize", type=int, default=16)
@click.option("--threads", default=4, type=int)
@click.pass_obj
def shatter_cmd(app, pointcloud, workers, tilesize, threads):
    """Insert data provided by POINTCLOUD into the silvimetric DATABASE"""
    with Client(n_workers=workers, threads_per_worker=threads) as client:
        client.get_versions(check=True)
        webbrowser.open(client.cluster.dashboard_link)
        # dask.config.set(scheduler="processes")
        shatter(pointcloud, app.database, tilesize, client=client)


@cli.command('extract')
# @click.option("--attributes", type=str, multiple=True,
#               default=["Z","NumberOfReturns","ReturnNumber",
#                        "HeightAboveGround","Intensity"])
@click.option("--attributes", "-a", multiple=True,
              help="List of attributes to include in Database",
              default=["Z","NumberOfReturns","ReturnNumber", "Intensity"])
@click.option("--outdir", "-o", type=str, required=True)
@click.pass_obj
def extract_cmd(app, attributes, outdir):
    """Extract silvimetric metrics from DATABASE """

    extract(app.database, outdir, attributes)


if __name__ == "__main__":
    cli()