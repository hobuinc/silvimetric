import numpy as np
from uuid import UUID

from ..resources import Storage, Bounds

def check_values(start_time, end_time, bounds, name):
    if start_time is not None and not isinstance(start_time, np.datetime64):
        raise TypeError(f'Incorrect type of "start_time" argument.')
    if end_time is not None and not isinstance(end_time, np.datetime64):
        raise TypeError(f'Incorrect type of "end_time" argument.')
    if bounds is not None and not isinstance(bounds, Bounds):
        raise TypeError(f'Incorrect type of "bounds" argument.')
    if name is not None:
        try:
            UUID(name)
        except:
            raise TypeError(f'Incorrect type of "name" argument.')

def info(tdb_dir, start_time:np.datetime64=None, end_time:np.datetime64=None,
          bounds:Bounds=None, name:str=None):
    """Print info about Silvimetric database"""
    check_values(start_time, end_time, bounds, name)

    with Storage.from_db(tdb_dir) as tdb:
        if start_time is None:
            start_time = np.datetime64(0, 'ms')
        if end_time is None:
            end_time = np.datetime64('now')
        if bounds is None:
            bounds = tdb.config.root
        if name is not None:
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