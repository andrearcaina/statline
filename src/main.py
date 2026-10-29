from fastapi import FastAPI

from src.api.handlers import Handler
from src.config import Config
from src.logger import Logger
from src.middleware import request_logger


class Server:
    def __init__(self) -> None:
        self.config = Config()
        self.logger = Logger(level=self.config.log_level)
        self.api = FastAPI(
            title="Statline API",
            version="0.0.0",
        )
        # custom structured logging middleware (uvicorn default sucks)
        self.api.middleware("http")(request_logger(self.logger))
        # sample API handler
        self.handler = Handler()
        self.api.include_router(self.handler.router)
        self.logger.info("server initialized")


server = Server()
app = server.api
