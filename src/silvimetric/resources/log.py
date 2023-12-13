"""
log.py
Project: CRREL-NEGGS University of Houston Collaboration
Date: February 2021

A module for setting up logging.
"""
import logging
import pathlib
import os
from typing import Any
from typing import Dict
from typing import TYPE_CHECKING

from .config import ApplicationConfig

try:
    import websocket
    from pythonjsonlogger import jsonlogger
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
            if log_record.get("level"):
                log_record["level"] = log_record["level"].upper()
            else:
                log_record["level"] = record.levelname

            if log_record.get("type") is None:
                log_record["type"] = "log_message"
            return None

    class WebSocketHandler(logging.Handler):
        def __init__(self, level: str, websocket: "websocket.WebSocket") -> None:
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
    def __init__(self, config: ApplicationConfig):
        """
        Creates logging formatting and structure

        Parameters
        ----------
        config:
            Application config representing the runtime config 
        """

        self.logger = logging.getLogger("silvimetric")
        self.logger.setLevel(config.log_level)
        self.config = config
        self.logfilename = "silvimetric-log.txt"


        # File Handler for Logging
        log_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )

        # We only use a file handler if user specified a logdir
        if config.logdir:
            logpath = pathlib.Path(config.logdir)
            
            # make the log directory if the user specified but 
            # it doesn't exist
            if not logpath.exists():
                logpath.mkdir()

            self.logfilename = str(logpath / self.logfilename )
            file_handler = logging.FileHandler( self.logfilename )

            file_handler.setLevel(config.log_level)
            file_handler.setFormatter(log_format)
            self.logger.addHandler(file_handler)

        if WebSocketHandler:
            self.relay = None

        # Supplemental Handler
        if config.logtype == "rich":
            from rich.logging import RichHandler

            log_handler = RichHandler()
        elif config.logtype == "websocket" and WebSocketHandler:
            formatter = CustomJsonFormatter()
            self.relay = websocket.WebSocket()
            url = f'ws://{config["WEBSOCKET_URL"]}/websocket'
            try:
                self.relay.connect(url)
            except ConnectionRefusedError as err:
                raise ConnectionRefusedError(f"Connection Refused to {url}")
            log_handler = WebSocketHandler("DEBUG", websocket=self.relay)
            log_handler.setFormatter(formatter)
        else:
            # if the user didn't specify a log dir, we just do the StreamHandler
            if not config.logdir:
                log_handler = logging.StreamHandler()
                log_handler.setFormatter(log_format)
                self.logger.addHandler(log_handler)

    def __del__(self) -> None:
        """Any special cleanups?"""
        if WebSocketHandler:
            if isinstance(self.logger, WebSocketHandler):
                self.logger.close()
    
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

        if self.config.logdir:
            logstring = f"{self.logfilename}"
        else:
            logstring = "stdout"

        return f"SilviMetric logging {id(self)} @ '{logstring}'"

