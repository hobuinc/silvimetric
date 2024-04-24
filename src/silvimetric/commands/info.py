from datetime import datetime
from uuid import UUID
from typing import Union

from .. import Storage, Bounds

def check_values(start_time: datetime, end_time: datetime, bounds: Bounds,
        name: Union[UUID, str]):
    """
    Validate arguments for info command.

    :param start_time: Starting datetime object.
    :param end_time: Ending datetime object.
    :param bounds: Bounds to query by.
    :param name: Name to query by.
    :raises TypeError: Incorrect type of start_time argument.
    :raises TypeError: Incorrect type of end_time argument.
    :raises TypeError: Incorrect type of bounds argument.
    :raises TypeError: Incorrect type of name argument.
    :raises TypeError: Incorrect type of name argument.
    :meta public:
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

    :param tdb_dir: TileDB database directory path.
    :param start_time: Process starting time query, defaults to None
    :param end_time: Process ending time query, defaults to None
    :param bounds: Bounds query, defaults to None
    :param name: Name query, defaults to None
    :return: Returns json object containing information on database.
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