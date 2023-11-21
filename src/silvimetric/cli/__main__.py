import click
import logging
import json
import dask
from dask.distributed import Client
import webbrowser

from silvimetric.app import Application
from silvimetric.storage import Storage
from silvimetric.shatter import shatter
from silvimetric.extract import extract

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
        database = database,
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

@cli.command('initialize')
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.option("--attributes", "-a", multiple=True,
              help="List of attributes to include in Database",
              default=["Z","NumberOfReturns","ReturnNumber", "Intensity"])
            #   default=["Z","NumberOfReturns","ReturnNumber",
            #            "HeightAboveGround","Intensity"])
#TODO support more bounds types in future
@click.option("--bounds", help="""Bounds to limit all Silvimetric processing. \
              '[minx,miny(,minz),maxx,maxy(,maxz)]'""", required=True)
@click.option("--crs", help="Coordinate reference system of the data", required=True)
@click.pass_obj
def initialize(app: Application, resolution: float, bounds: str, attributes: str, crs: str):
    import pyproj
    """Initialize silvimetrics DATABASE

    :param resolution: Resolution of a cell in dataset units, default to 30.0
    :type resolution: float, optional
    """
    try:
        pyproj.CRS.from_user_input(crs)
    except BaseException as e:
        raise Exception(f"Invalid CRS entered. {crs}")

    try:
        b = json.loads(bounds)
    except json.JSONDecodeError:
        raise Exception(f"""Invalid bounding box {bounds}. Must be in shape\
                         '[minx,miny(,minz),maxx,maxy(,maxz)]'""")

    print(f"Creating database at {app.database}")
    from silvimetric.cli.initialize import initialize as initializeFunction
    initializeFunction(app, resolution, b, attributes, crs)

@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--workers", type=int, default=12)
@click.option("--tilesize", type=int, default=16)
@click.option("--threads", default=4, type=int)
@click.option("--redisurl", type=str)
@click.pass_obj
def shatter_cmd(app, pointcloud, workers, tilesize, threads):
    """Insert data provided by POINTCLOUD into the silvimetric DATABASE"""

    #TODO if redisurl is not specified then it's local threads
    with Client(n_workers=workers, threads_per_worker=threads) as client:
        client.get_versions(check=True)
        webbrowser.open(client.cluster.dashboard_link)
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
