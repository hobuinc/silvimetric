import pathlib

from silvimetric.resources import Storage, StorageConfig, ApplicationConfig

def initialize(app: ApplicationConfig, storage: StorageConfig):
    """
    Initialize a Silvimetric TileDB instance for a given StorageConfig instance

    Parameters
    ----------
    StorageConfig :  StorageConfig

    """

    app.log.logger.debug(f"Initializing SilviMetric Database at '{storage.tdb_dir}'")

    s = Storage.create(storage)
    return s
