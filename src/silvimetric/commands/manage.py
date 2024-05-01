from .. import Storage, ShatterConfig
from .info import info
from .shatter import shatter

def delete(tdb_dir: str, name:str) -> ShatterConfig:
    """
    Delete Shatter process from database and return config for that process.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of the Shatter process.
    :raises KeyError: Shatter process with ID does not exist.
    :raises ValueError: Shatter process with ID is missing a time reservation
    :return: Config of process that was deleted.
    """
    res = info(tdb_dir=tdb_dir, name=name)

    try:
        config = ShatterConfig.from_dict(res['history'][0])
    except LookupError:
        raise KeyError(f'Shatter process with ID {name} does not exist')

    try:
        time_slot = config.time_slot
    except KeyError:
        raise ValueError(f'Shatter process with ID {name} is missing a time reservation.')

    storage = Storage.from_db(tdb_dir)
    return storage.delete(time_slot)

def restart(tdb_dir: str, name: str) -> int:
    """
    Delete shatter process from database and run it again with the same config.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """

    cfg = delete(tdb_dir, name)
    return shatter(cfg)

def resume(tdb_dir: str, name: str) -> int:
    """
    Resume partially completed shatter process. Process must partially completed
    and have an already established time slot.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """
    res = info(tdb_dir=tdb_dir, name=name)
    assert len(res['history']) == 1
    config = res['history'][0]
    return shatter(config)