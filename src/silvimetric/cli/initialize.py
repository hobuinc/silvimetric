import logging
import pathlib

from silvimetric.app import Application
from silvimetric.storage import Storage, StorageConfig
from silvimetric.initialize import InitCommand

logger = logging.getLogger(__name__)

def initialize(config: StorageConfig):
    """
    Initialize a Silvimetric TileDB instance for a given Application instance

    Parameters
    ----------
    StorageConfig : Storage StorageConfig

    """

    logger.debug(f"Initializing application with {config} settings")

    return Storage.create(config)
