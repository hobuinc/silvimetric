from .. import Storage, ShatterConfig
from .info import info
from .shatter import shatter
from ..resources.log import Log


def get_logger(log):
    if log is None:
        return Log('INFO')
    else:
        return log


def delete(storage: str|Storage, name: str, log: Log = None) -> ShatterConfig:
    """
    Delete Shatter process from database and return config for that process.

    :param storage: SilviMetric Storage or TileDB directory path.
    :param name: UUID name of the Shatter process.
    :raises KeyError: Shatter process with ID does not exist.
    :raises ValueError: Shatter process with ID is missing a time reservation
    :return: Config of process that was deleted.
    """

    # NOTE: this process is not perfect due to the technology employed by
    # tiledb. After consolidations, this should eventually show up as working
    # but may not immediately show results

    logger = get_logger(log)
    if isinstance(storage, str):
        storage = Storage.from_db(storage)

    res = info(storage=storage, name=name)

    try:
        config = ShatterConfig.from_dict(res['history'][0])
    except LookupError as e:
        raise KeyError(f'Shatter process with ID {name} does not exist') from e

    try:
        time_slot = config.time_slot
    except KeyError as e:
        raise ValueError(
            f'Shatter process with ID {name} is missing a time slot.'
        ) from e

    logger.info(f'Deleting task {name}.')
    return storage.delete(time_slot)


def restart(storage: str|Storage, name: str, log: Log = None) -> int:
    """
    Delete shatter process from database and run it again with the same config.

    :param storage: SilviMetric Storage or TileDB directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """
    if isinstance(storage, str):
        storage = Storage.from_db(storage)

    logger = get_logger(log)

    cfg = delete(storage=storage, name=name, log=log)
    logger.info(f'Restarting task {name} with same config.')
    return shatter(cfg)


def resume(storage: str|Storage, name: str, log: Log = None) -> int:
    """
    Resume partially completed shatter process. Process must partially completed
    and have an already established time slot.

    :param storage: SilviMetric Storage or TileDB directory path.
    :param name: UUID name of Shatter process.
    :return: Point count of the restarted shatter process.
    """
    if isinstance(storage, str):
        storage = Storage.from_db(storage)

    logger = get_logger(log)

    logger.info(f'Resuming task {name}.')
    res = info(storage=storage, name=name)
    if len(res['history']) > 1:
        raise ValueError(f'More than one config found with name {name}.')
    if len(res['history']) < 0:
        raise ValueError(f'No config found with name {name}.')

    config = res['history'][0]
    if isinstance(config, dict):
        # if the tdb_dir is different, don't continue writing to the old one
        config['tdb_dir'] = storage.config.tdb_dir
        config = ShatterConfig.from_dict(config)
    return shatter(config)
