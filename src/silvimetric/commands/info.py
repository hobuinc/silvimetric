import json
import numpy as np

from ..resources import Storage, Bounds

def info(tdb_dir, start_time=None, end_time=None, bounds: Bounds=None, sh_name: str=None):
    """Print info about Silvimetric database"""
    with Storage.from_db(tdb_dir) as tdb:
        if start_time is None:
            start_time = 0
        if end_time is None:
            end_time = np.datetime64('now').astype('datetime64[ms]').astype('int')
        if bounds is None:
            bounds = tdb.config.root

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json()
        }

        try:
            history = tdb.get_history(start_time, end_time, bounds)

            if bool(history) and isinstance(history, list):
                history = [ json.loads(h) for h in history ]
            elif bool(history):
                history = [ json.loads(history) ]
            else:
                history = [ ]

            info['history'] = history
        except KeyError:
            history = {}

        return info