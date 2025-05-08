"""
log.py
Project: CRREL-NEGGS University of Houston Collaboration
Date: February 2021

A module for setting up logging.
"""

import logging
import pathlib
import sys

from typing_extensions import Any, Dict, Optional

try:
    import websocket
    from pythonjsonlogger import json as jsonlogger
except ImportError:
    WebSocketHandler = None
    pass
else:

    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def add_fields(
            self,
            log_record: Dict[str, Any],
            record: logging.LogRecord,
            message_dict: Dict[str, Any],
        ) -> None:
            super().add_fields(log_record, record, message_dict)
            if log_record.get('level'):
                log_record['level'] = log_record['level'].upper()
            else:
                log_record['level'] = record.levelname

            if log_record.get('type') is None:
                log_record['type'] = 'log_message'
            return None

    class WebSocketHandler(logging.Handler):
        def __init__(
            self, level: str, websocket: 'websocket.WebSocket'
        ) -> None:
            super().__init__(level)
            self.ws = websocket
            # TODO: check if websocket is already connected?

        def emit(self, record: logging.LogRecord) -> None:
            msg = self.format(record)
            _ = self.ws.send(msg)
            return None

        def close(self) -> None:
            self.ws.close()
            return super().close()


class Log:
    def __init__(
        self,
        log_level: int,
        logdir: Optional[str] = None,
        logtype: str = 'stream',
        logfilename: str = 'silvimetric-log.txt',
    ):
        """
        Creates logging formatting and structure

        :param config: Application config representing the runtime config
        """

        # need to be careful not to pull logging from previous runs
        self.logger = logging.getLogger('silvimetric')
        self.logdir = logdir
        if logdir:
            self.logtype = 'file'
        else:
            self.logtype = logtype
        self.logfilename = logfilename

        # do not recreate handlers if they're already present
        if self.logger.handlers:
            self.log_level = self.logger.level
            return

        self.log_level = log_level
        self.logger.setLevel(log_level)

        # File Handler for Logging
        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d'
            ' - %(message)s'
        )

        # We only use a file handler if user specified a logdir
        if self.logdir:
            logpath = pathlib.Path(self.logdir)

            # make the log directory if the user specified but
            # it doesn't exist
            if not logpath.exists():
                logpath.mkdir()

            logfilename = str(logpath / self.logfilename)
            file_handler = logging.FileHandler(logfilename)

            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(log_format)
            self.logger.addHandler(file_handler)
        else:
            log_handler = logging.StreamHandler(stream=sys.stdout)
            log_handler.setFormatter(log_format)
            self.logger.addHandler(log_handler)

    def to_json(self):
        return {
            'logdir': self.logdir,
            'log_level': self.log_level,
            'logtype': self.logtype,
            'logfilename': self.logfilename,
        }

    def __del__(self) -> None:
        """Any special cleanups?"""
        if WebSocketHandler:
            if isinstance(self.logger, WebSocketHandler):
                self.logger.close()

    def __eq__(self, other):
        return self.to_json() == other.to_json()

    def warning(self, msg: str):
        """Forward warning messages down to logger"""
        self.logger.warning(msg)

    def debug(self, msg: str):
        """Forward debug messages down to logger"""
        self.logger.debug(msg)

    def info(self, msg: str):
        """Forward info messages down to logger"""
        self.logger.info(msg)

    def __repr__(self):
        """Print out where our logs are going"""

        if self.logdir:
            logstring = f'{self.logfilename}'
        else:
            logstring = 'stdout'

        return f"SilviMetric logging {id(self)} @ '{logstring}'"
