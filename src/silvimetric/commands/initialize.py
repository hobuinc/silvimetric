from ..resources import Storage, StorageConfig

def initialize(storage: StorageConfig):
    """
    Initialize a Silvimetric TileDB instance for a given StorageConfig instance

    Parameters
    ----------
    storage : StorageConfig

    Returns
    -------
    Storage
    """

    storage.log.debug(f"Initializing SilviMetric Database at '{storage.tdb_dir}'")

    s = Storage.create(storage)
    return s
