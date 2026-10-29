from fastapi import APIRouter

from src.api.types import HealthResponse


class Handler:
    def __init__(self) -> None:
        self.router = APIRouter()
        self.router.add_api_route(
            "/healthz",
            self.healthz,
            methods=["GET"],
            response_model=HealthResponse,
        )

        self.router.add_api_route(
            "/hello",
            self.hello,
            methods=["GET"],
            response_model=str,
        )

    def healthz(self) -> HealthResponse:
        return HealthResponse(status="ok")

    def hello(self) -> str:
        return "Hello, World!"
