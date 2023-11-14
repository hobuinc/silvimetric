import logging

from silvistat.app import Application

logger = logging.getLogger(__name__)

def initialize(application):
    """Initialize a Silvistats TileDB instance for a given Application instance"""

    logger.debug(f"Initializing application with {application} settings")
    pass
