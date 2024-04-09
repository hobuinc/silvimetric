from ..resources import Storage, ShatterConfig
from .info import info
from .shatter import shatter

def delete(tdb_dir: str, name:str) -> ShatterConfig:
    """
    Delete Shatter process from database

    Parameters
    ----------
    tdb_dir : str
    name : str

    Returns
    -------
    ShatterConfig
        Config of deleted Shatter process

    Raises
    ------
    KeyError
        Shatter process doesn't exist
    ValueError
        Shatter process is missing time slot
    """
    res = info(tdb_dir=tdb_dir, name=name)

    try:
        config = ShatterConfig.from_dict(res['history'][0])
    except LookupError:
        raise KeyError(f'Shatter process with ID {name} does not exist')

    try:
        time_slot = config.time_slot
    except KeyError:
        raise ValueError(f'Shatter process with ID {name} missing time reservation.')

    storage = Storage.from_db(tdb_dir)
    return storage.delete(time_slot)

def restart(tdb_dir: str, name: str) -> int:
    """
    Delete shatter process from database and run it again with the same config

    Parameters
    ----------
    tdb_dir : str
    name : str

    Returns
    -------
    int
        Point count
    """

    cfg = delete(tdb_dir, name)
    return shatter(cfg)

def resume(tdb_dir: str, name: str) -> int:
    """
    Resume partially completed shatter process

    Parameters
    ----------
    tdb_dir : str
    name : str

    Returns
    -------
    int
        Point count
    """
    res = info(tdb_dir=tdb_dir, name=name)
    assert len(res['history']) == 1
    config = res['history'][0]
    return shatter(config)