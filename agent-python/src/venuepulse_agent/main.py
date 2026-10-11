"""FastAPI app factory. Run with: uvicorn venuepulse_agent.main:app"""

import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from venuepulse_agent.api import health
from venuepulse_agent.config import Settings, get_settings
from venuepulse_agent.errors import AgentError
from venuepulse_agent.logging import configure_logging, get_logger

REQUEST_ID_HEADER = "X-Request-ID"

log = get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, json=settings.log_json)

    app = FastAPI(title="VenuePulse Agent", version="0.1.0")
    app.include_router(health.router)

    @app.middleware("http")
    async def request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex
        structlog.contextvars.bind_contextvars(request_id=request_id)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            log.exception("unhandled error", method=request.method, path=request.url.path)
            response = JSONResponse(
                {
                    "type": "about:blank",
                    "title": "Internal Server Error",
                    "status": 500,
                    "request_id": request_id,
                },
                status_code=500,
                media_type="application/problem+json",
            )
        finally:
            structlog.contextvars.unbind_contextvars("request_id")
        response.headers[REQUEST_ID_HEADER] = request_id
        if request.url.path not in ("/healthz", "/readyz"):
            log.info(
                "request",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration_ms=round((time.perf_counter() - start) * 1000, 1),
            )
        return response

    @app.exception_handler(AgentError)
    async def agent_error(request: Request, exc: AgentError) -> JSONResponse:
        log.warning("agent error", code=exc.code, error=exc.message, retryable=exc.retryable)
        return JSONResponse(
            {
                "type": f"urn:venuepulse:agent:{exc.code}",
                "title": exc.code,
                "status": 502,
                "detail": exc.message,
                "retryable": exc.retryable,
                "request_id": request.headers.get(REQUEST_ID_HEADER),
            },
            status_code=502,
            media_type="application/problem+json",
        )

    log.info("agent configured", model=settings.agent_model)
    return app


app = create_app()
