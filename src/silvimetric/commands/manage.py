from ..resources import Storage, ShatterConfig
from .info import info
from .shatter import shatter

def delete(tdb_dir: str, name:str):
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

def restart(tdb_dir: str, name: str):
    cfg = delete(tdb_dir, name)
    shatter(cfg)

def resume(tdb_dir: str, name: str):
    res = info(tdb_dir=tdb_dir, name=name)
    assert len(res['history']) == 1
    config = res['history'][0]
    shatter(config)