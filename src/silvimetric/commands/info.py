import json
import numpy as np

from ..resources import Storage, Bounds, Extents

def info(tdb_dir, start_time=None, end_time=None, bounds: Bounds=None):
    """Print info about Silvimetric database"""
    with Storage.from_db(tdb_dir) as tdb:

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        # We don't have this until stuff has been put into the database
        try:
            shatter = json.loads(tdb.getMetadata('shatter'))
        except KeyError:
            shatter = {}

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json(),
            'shatter': shatter
        }
        # TODO add bounds query
        # TODO shatter process name filtering
        # TODO time filtering

        try:
            if start_time is None:
                start_time = 0
            if end_time is None:
                end_time == np.datetime64('now').astype('datetime64[ms]').astype('int')
            og_history = tdb.get_history(start_time, end_time)
            history = og_history['shatter']

            if isinstance(history, list):
                history = [ json.loads(h) for h in history ]
            else:
                history = [ json.loads(history) ]

            # filter
            history = [h for h in history if h]

            if bounds is not None:
                e = Extents(bounds, tdb.config.resolution, tdb.config.root)
                a = tdb[e.x1:e.x2, e.y1:e.y2]

            info['history'] = history
        except KeyError:
            history = {}

        return info