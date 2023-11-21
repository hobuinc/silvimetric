import logging
import pathlib

from silvimetric.app import Application
from silvimetric.storage import Storage, Configuration
from silvimetric.initialize import InitCommand 

logger = logging.getLogger(__name__)

def initialize(config: Configuration):
    """
    Initialize a Silvimetric TileDB instance for a given Application instance

    Parameters
    ----------
    configuration : Storage Configuration

    """

    logger.debug(f"Initializing application with {config} settings")

    return Storage.create(config)
