import logging
import pathlib

from silvimetric.app import Application
from silvimetric.storage import Storage
from silvimetric.bounds import Bounds
from silvimetric.initialize import InitCommand 
from pyproj import CRS

logger = logging.getLogger(__name__)

def initialize(application: Application, resolution: int, bounds: Bounds, atts: list[str], crs: CRS):
    """
    Initialize a Silvimetric TileDB instance for a given Application instance

    Parameters
    ----------
    application : Application

    resolution : int
        Resolution of an individual cell
    bounds : list[float]
        Bounds that the data will cover, in Bounding box form
    atts : list[str]
        List attributes to apply to Database
    dirname : str
        Path to Database location
    """

    logger.debug(f"Initializing application with {application} settings")

    return Storage.create(atts, resolution, bounds, application.database, crs)
