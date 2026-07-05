from .. import Storage, StorageConfig


def initialize(storage: StorageConfig):
    """
    Initialize a Silvimetric storage instance for a given StorageConfig.

    :param storage: :class:`silvimetric.resources.config.StorageConfig`.
    :return: :class:`silvimetric.resources.storage.Storage` database object.
    """

    s = Storage.create(storage)
    storage.log.info(f"Initialized SilviMetric Database at '{storage.tdb_dir}'")
    return s
