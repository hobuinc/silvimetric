import click
import logging
import json

from silvistat.app import Application

from silvistat.bounds import Bounds

logger = logging.getLogger(__name__)

@click.group()
@click.argument("database", type=click.Path(exists=False))
@click.option("--log-level", default="INFO", help="Log level (INFO/DEBUG)")
@click.option("--threads", default=4, type=int, help="Application thread count")
@click.option("--progress", default=True, type=bool, help="Report progress")
@click.pass_context
def cli(ctx, database, log_level, threads, progress):

    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level)

    # Log program args

    app = Application(
        log_level=log_level,
        threads=threads,
        progress = progress,
        database = database,
    )
    ctx.obj = app


@cli.command("info")
@click.pass_obj
def info(app):
    """Print info about Silvistats database"""
    pass

@cli.command('initialize')
@click.option("--resolution", type=float, help="Summary pixel resolution", default=30.0)
@click.option("--attributes", help="List of attributes to include in Database",
              default='["Z","NumberOfReturns","ReturnNumber","HeightAboveGround","Intensity"]')
#TODO support more bounds types in future
@click.option("--bounds", help="""Bounds to limit all Silvistat processing. \
              '[minx,miny(,minz),maxx,maxy(,maxz)]'""", required=True)
@click.pass_obj
def initialize(app: Application, resolution: float, bounds: str, attributes: str):
    """Initialize Silvistats DATABASE

    :param resolution: Resolution of a cell in dataset units, default to 30.0
    :type resolution: float, optional
    """
    try:
        b = json.loads(bounds)
    except json.JSONDecodeError:
        raise Exception(f"""Invalid bounding box {bounds}. Must be in shape\
                         '[minx,miny(,minz),maxx,maxy(,maxz)]'""")

    try:
        a = json.loads(attributes)
    except json.JSONDecodeError:
        raise Exception(f"""Invalid attributes list {attributes}. Must be in \
                        form '["Att1","Att2"(, ...)]'""")

    print(app.database)
    from silvistat.cli.initialize import initialize as initializeFunction
    initializeFunction(app, resolution, b, a)

@cli.command('shatter')
@click.argument("pointcloud", type=str)
@click.option("--workers", type=int, default=12)
@click.option("--groupsize", type=int, default=12)
@click.option("--attributes",  multiple=True, default = ["all"])
@click.pass_obj
def shatter(app, pointcloud, workers, groupsize, attributes):
    """Insert data provided by POINTCLOUD into the Silvistat DATABASE"""
    pass


@cli.command('extract')
@click.option("--attributes", type=str, multiple=True, default = ["all"])
@click.pass_obj
def extract(app, attributes, ):
    """Extract Silvistat metrics from DATABASE """
    pass

if __name__ == "__main__":
    cli()
