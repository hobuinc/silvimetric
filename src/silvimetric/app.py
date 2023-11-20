import logging
import pathlib
import os

from typing import Union, Optional
from urllib.parse import urlparse



class Application:
    def __init__(
            self,
            tdb_dir: str = None,
            log_level=logging.INFO,
            threads: int = 20,
            progress: bool = False,
    ) -> None:
        """_summary_

        Parameters
        ----------
        tdb_dir : str
            Tiledb Silvimetric database location
        log_level : logging.Level, optional
            Logging level to set doppkit application to, by default logging.INFO
        threads : int, optional
            Number of threads to use to download resources, by default 20
        """
        # need to assign the attribute
        self.tdb_dir = tdb_dir
        self.threads = threads
        self.progress = progress
        self.log_level = log_level

    def __repr__(self) -> str:
        return (
            "Silvimetric Application\n"
            f"Database location {self.database}\n"
        )
