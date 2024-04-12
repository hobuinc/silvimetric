from silvimetric.resources import Storage, StorageConfig

def initialize(storage: StorageConfig):
    """
    Initialize a Silvimetric TileDB instance for a given StorageConfig instance.

    :param storage: :class:`silvimetric.resources.config.StorageConfig`.
    :return: :class:`silvimetric.resources.storage.Storage` database object.
    """

    storage.log.info(f"Initializing SilviMetric Database at '{storage.tdb_dir}'")

    s = Storage.create(storage)
    return s
