from . import Storage

import logging
logger = logging.getLogger(__name__)

class InitCommand():
    def __init__(self, args):

        logger.debug(f"InitCommand invocation __init__")
        return Storage.create(args.atts, args.resolution, args.bounds, args.database, args.crs)