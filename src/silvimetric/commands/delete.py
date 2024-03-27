from uuid import UUID

from ..resources import Storage
from .info import info

def delete(tdb_dir: str, name:str):
    res = info(tdb_dir=tdb_dir, name=name)
    print(res)