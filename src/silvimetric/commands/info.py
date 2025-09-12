from datetime import datetime
from uuid import UUID
from typing_extensions import Union

from .. import Storage, Bounds
from typing import Optional


def info(
    tdb_dir: str,
    timestamp: Optional[tuple[int, int]] = None,
    bounds: Optional[Bounds] = None,
    name: Optional[Union[str, UUID]] = None,
    concise: Optional[bool] = False,
) -> dict:
    """
    Collect information about database in current state

    :param tdb_dir: TileDB database directory path.
    :param start_time: Process starting time query, defaults to None
    :param end_time: Process ending time query, defaults to None
    :param bounds: Bounds query, defaults to None
    :param name: Name query, defaults to None
    :return: Returns json object containing information on database.
    """
    with Storage.from_db(tdb_dir) as tdb:
        if bounds is None:
            bounds = tdb.config.root
        if name is not None:
            if isinstance(name, str):
                name = UUID(name)

        # We always have these
        meta = tdb.get_config()
        atts = tdb.get_attributes()

        info = {
            'attributes': [a.to_json() for a in atts],
            'metadata': meta.to_json(),
        }

        try:
            history = tdb.get_history(timestamp, bounds, name, concise)
            if bool(history) and isinstance(history, list):
                history = [h for h in history]
            elif bool(history):
                history = [history]
            else:
                history = []

            info['history'] = history
        except KeyError:
            info['history'] = {}
    # remove unnecessary keys for printing to logs
    if concise:
        info['metadata'].pop('attrs')
        info['metadata'].pop('debug')
        info['metadata'].pop('log')

    return info
