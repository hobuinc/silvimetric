import json

from ..resources import Storage

def info(tdb_dir: str, history: bool):
    """
    Log information on the database

    Parameters
    ----------
    tdb_dir : str
    history : bool
    """
    with Storage.from_db(tdb_dir) as tdb:

        # We always have these
        meta = tdb.getConfig()
        atts = tdb.getAttributes()

        # We don't have this until stuff has been put into the database
        try:
            shatter = json.loads(tdb.getMetadata('shatter'))

            # I don't know what this is?
        except KeyError:
            shatter = {}

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json(),
            'shatter': shatter
        }
        if history:
            try:
                # I don't know what this is? – hobu
                history = tdb.get_history()['shatter']
                if isinstance(history, list):
                    history = [ json.loads(h) for h in history ]
                else:
                    history = json.loads(history)
                info ['history'] = history
            except KeyError:
                history = {}
        print(json.dumps(info, indent=2))