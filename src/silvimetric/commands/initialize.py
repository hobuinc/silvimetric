from silvimetric.resources import Storage, StorageConfig

def initialize(storage: StorageConfig):
    """
    Initialize a Silvimetric TileDB instance for a given StorageConfig instance

    Parameters
    ----------
    StorageConfig :  StorageConfig

    """

    storage.log.info(f"Initializing SilviMetric Database at '{storage.tdb_dir}'")

    s = Storage.create(storage)
    return s
