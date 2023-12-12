import click
import logging
import json
import dask
from dask.distributed import Client
import webbrowser
import pyproj

from silvimetric.app import Application
from silvimetric.resources import Storage, Bounds
from silvimetric.resources import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric.commands import shatter, extract

logger = logging.getLogger(__name__)


from json import JSONEncoder
class MyEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

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
    with Storage.from_db(app.tdb_dir) as tdb:

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        # We don't have this until stuff has been put into the database
        try:
            shatter = tdb.getMetadata('shatter')

            # I don't know what this is?
        except KeyError:
            shatter = {}

        try:
            # I don't know what this is? – hobu
            history = tdb.get_history()['shatter']

        except KeyError:
            history = {}
        info = {
            'attributes': atts,
            'metadata': meta.to_json(),
            'shatter': shatter,
            'history': history
        }
        print(json.dumps(info, indent=2, cls=MyEncoder))

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

@cli.command('initialize')
@click.argument("bounds", type=BoundsParamType())
@click.argument("crs", type=CRSParamType())
@click.option("--attributes", "-a", multiple=True,
              help="List of attributes to include in Database")
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.option("--name", "-a", type=str,
              help="Working name for this SilviMetric DB (random one given if not specified)")
@click.pass_obj
def initialize(app: Application, bounds: Bounds, crs: pyproj.CRS, attributes: list[str], resolution: float, name: str):
    """Initialize silvimetrics DATABASE
    """

    print(f"Creating database at {app.tdb_dir}")
    from silvimetric.cli.initialize import initialize as initializeFunction
    config = StorageConfig(app.tdb_dir, bounds, resolution, crs)
    if name:
        config.name = name
    if attributes:
        config.attrs = attributes
    if resolution:
        config.resolution = resolution

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
        config = ShatterConfig(tdb_dir=app.tdb_dir, filename=pointcloud,
            tile_size=tilesize)
        webbrowser.open(client.cluster.dashboard_link)
        shatter(config, client)


@cli.command('extract')
# @click.option("--attributes", type=str, multiple=True,
#               default=["Z","NumberOfReturns","ReturnNumber",
#                        "HeightAboveGround","Intensity"])
@click.option("--attributes", "-a", multiple=True,
              help="List of attributes to include in output. Default to \
                what's in TileDB.", default=[])
@click.option("--metrics", "-m", multiple=True,
              help="List of metrics to include in output. Default to \
                what's in TileDB.", default=[])
@click.option("--outdir", "-o", type=str, required=True)
@click.pass_obj
def extract_cmd(app, attributes, metrics, outdir):
    """Extract silvimetric metrics from DATABASE """

    config = ExtractConfig(app.tdb_dir, outdir, attributes, metrics)
    extract(config)


if __name__ == "__main__":
    cli()
