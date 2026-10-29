from collections.abc import Awaitable, Callable

from fastapi import Request, Response

from src.logger import Logger


def request_logger(
    logger: Logger,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    async def middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:

        try:
            response = await call_next(request)
        except Exception as error:
            logger.error(
                "http request failed",
                method=request.method,
                path=request.url.path,
                status_code=500,
                error=str(error),
            )
            raise

        logger.info(
            "http request",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
        )
        return response

    return middleware
