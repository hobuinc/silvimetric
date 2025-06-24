from .. import Storage, ShatterConfig
from .info import info
from .shatter import shatter
from ..resources.log import Log


def get_logger(log):
    if log is None:
        return Log('INFO')
    else:
        return log


def delete(tdb_dir: str, name: str, log: Log = None) -> ShatterConfig:
    """
    Delete Shatter process from database and return config for that process.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of the Shatter process.
    :raises KeyError: Shatter process with ID does not exist.
    :raises ValueError: Shatter process with ID is missing a time reservation
    :return: Config of process that was deleted.
    """

    logger = get_logger(log)

    res = info(tdb_dir=tdb_dir, name=name)

    try:
        config = ShatterConfig.from_dict(res['history'][0])
    except LookupError as e:
        raise KeyError(f'Shatter process with ID {name} does not exist') from e

    try:
        time_slot = config.time_slot
    except KeyError as e:
        raise ValueError(
            f'Shatter process with ID {name} is missing a timestamp.'
        ) from e

    storage = Storage.from_db(tdb_dir)

    logger.info(f'Deleting task {name}.')
    return storage.delete(time_slot)


def restart(tdb_dir: str, name: str, log: Log = None) -> int:
    """
    Delete shatter process from database and run it again with the same config.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """

    logger = get_logger(log)

    cfg = delete(tdb_dir, name)
    logger.info(f'Restarting task {name} with same config.')
    return shatter(cfg)


def resume(tdb_dir: str, name: str, log: Log = None) -> int:
    """
    Resume partially completed shatter process. Process must partially completed
    and have an already established time slot.

    :param tdb_dir: TileDB database directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """

    logger = get_logger(log)

    logger.info(f'Resuming task {name}.')
    res = info(tdb_dir=tdb_dir, name=name)
    assert len(res['history']) == 1
    config = res['history'][0]
    return shatter(config)
