import logging

from silvistat.app import Application
from silvistat.storage import Storage

logger = logging.getLogger(__name__)

def initialize(application: Application, resolution: int, bounds: list[float], atts: list[str]):
    """
    Initialize a Silvistats TileDB instance for a given Application instance

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
    storage = Storage()
    storage.create(atts, resolution, bounds, application.database)
