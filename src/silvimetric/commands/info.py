from datetime import datetime
from uuid import UUID
from typing import Union

from ..resources import Storage, Bounds

def check_values(start_time: datetime, end_time: datetime, bounds: Bounds,
        name: Union[UUID, str]):
    """
    Validate arguments for info

    Parameters
    ----------
    start_time : datetime
    end_time : datetime
    bounds : Bounds
    name : Union[UUID, str]

    Raises
    ------
    TypeError
        Incorrect start_time type
    TypeError
        Incorrect end_time type
    TypeError
        Incorrect bounds type
    TypeError
        Incorrect name type
    TypeError
        Incorrect name type
    """
    if start_time is not None and not isinstance(start_time, datetime):
        raise TypeError(f'Incorrect type of "start_time" argument.')
    if end_time is not None and not isinstance(end_time, datetime):
        raise TypeError(f'Incorrect type of "end_time" argument.')
    if bounds is not None and not isinstance(bounds, Bounds):
        raise TypeError(f'Incorrect type of "bounds" argument.')
    if name is not None:
        if isinstance(name, UUID):
            pass
        elif isinstance(name, str):
            try:
                    UUID(name)
            except:
                raise TypeError(f'Incorrect type of "name" argument.')
        else:
            raise TypeError(f'Incorrect type of "name" argument.')

def info(tdb_dir:str, start_time:datetime=None, end_time:datetime=None,
          bounds:Bounds=None, name:Union[str, UUID]=None) -> dict:
    """
    Collect information about database in current state

    Parameters
    ----------
    tdb_dir : str
        Tiledb directory
    start_time : datetime, optional
        Start time range, by default None
    end_time : datetime, optional
        End time range, by default None
    bounds : Bounds, optional
        Bounds filter, by default None
    name : Union[str, UUID], optional
        Name of shatter process, by default None

    Returns
    -------
    dict
        Database information dictionary
    """
    check_values(start_time, end_time, bounds, name)

    with Storage.from_db(tdb_dir) as tdb:
        if start_time is None:
            start_time = datetime.fromtimestamp(0)
        if end_time is None:
            end_time = datetime.now()
        if bounds is None:
            bounds = tdb.config.root
        if name is not None:
            if isinstance(name, str):
                name = UUID(name)

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json()
        }

        try:
            history = tdb.get_history(start_time, end_time, bounds, name)

            if bool(history) and isinstance(history, list):
                history = [ h for h in history ]
            elif bool(history):
                history = [ history ]
            else:
                history = [ ]

            info['history'] = history
        except KeyError:
            history = {}

    return info