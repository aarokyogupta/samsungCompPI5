import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import gc
import importlib
import inspect
import json
import logging
import os
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
import secrets
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send
import sys
import time
import traceback
from typing import Annotated, Any, AsyncGenerator, Optional
import uuid
import yaml

# Allow "python api/main.py" as well as "uvicorn api.main:app" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiEngine.graphWorkflow import compileWorkflow  # noqa: E402
from aiEngine.llmProvider import LLMProvider  # noqa: E402
from database.dbManager import DatabaseManager  # noqa: E402

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

API_CONFIG: dict = config.get("api", {}) or {}

API_TITLE: str = str(API_CONFIG.get("title", "Conservation AI Edge API"))
API_VERSION: str = str(API_CONFIG.get("version", "1.0.0"))
DEFAULT_ORIGINS: list[str] = ["http://localhost:3000", "https://dashboard.conservation-edge.local"]
WARM_UP_MODES: tuple[str, ...] = ("background", "blocking", "off")
REQUEST_ID_HEADER: str = "X-Request-ID"
API_KEY_HEADER: str = "X-API-Key"

logger = logging.getLogger("icmis.api")


# Settings: config.yaml gives the defaults, ICMIS_* environment variables (or .env) override them

class RouterModule(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    module: str
    prefix: str = ""
    tags: list[str] = Field(default_factory=list)
    require_auth: bool = Field(False, validation_alias=AliasChoices("requireAuth", "require_auth"))


def parseList(value: Any) -> Any:
    # Accepts a YAML list, a JSON array string or a comma-separated string (the easiest form for env vars)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            return json.loads(text)
        return [item.strip() for item in text.split(",") if item.strip()]
    return value


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ICMIS_",
        env_file=os.path.join(PROJECT_ROOT, ".env"),
        extra="ignore",
    )

    host: str = str(API_CONFIG.get("host", "0.0.0.0"))
    port: int = int(API_CONFIG.get("port", 8000))
    debug: bool = bool(API_CONFIG.get("debug", False))
    log_level: str = str(API_CONFIG.get("logLevel", "INFO"))
    allowed_origins: Annotated[list[str], NoDecode] = list(API_CONFIG.get("allowedOrigins", DEFAULT_ORIGINS) or [])
    allowed_origin_regex: Optional[str] = API_CONFIG.get("allowedOriginRegex") or None
    api_prefix: str = str(API_CONFIG.get("apiPrefix", "/api/v1"))
    frontend_directory: str = str(API_CONFIG.get("frontendDirectory", "webDashboard/dist"))
    enable_docs: bool = bool(API_CONFIG.get("enableDocs", True))
    require_database: bool = bool(API_CONFIG.get("requireDatabase", True))
    llm_warm_up: str = str(API_CONFIG.get("llmWarmUp", "background"))
    llm_warm_up_timeout_sec: float = float(API_CONFIG.get("llmWarmUpTimeoutSec", 180.0))
    unload_llm_on_shutdown: bool = bool(API_CONFIG.get("unloadLlmOnShutdown", True))
    router_modules: list[RouterModule] = [RouterModule.model_validate(entry) for entry in API_CONFIG.get("routerModules", []) or []]
    # Only ever read from the environment / .env so the key never lands in config.yaml
    api_key: Optional[SecretStr] = None

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def splitOrigins(cls, value: Any) -> Any:
        return parseList(value)

    @field_validator("allowed_origins")
    @classmethod
    def rejectWildcard(cls, value: list[str]) -> list[str]:
        # Browsers refuse credentialed responses to "*", so it would silently break the dashboard
        if "*" in value:
            raise ValueError("allowed_origins cannot contain '*' because credentials are allowed; list origins explicitly.")
        return [origin.rstrip("/") for origin in value]

    @field_validator("api_prefix")
    @classmethod
    def normalizePrefix(cls, value: str) -> str:
        value = "/" + value.strip().strip("/")
        if value == "/":
            raise ValueError("api_prefix cannot be '/' because the frontend is served from the root.")
        return value

    @field_validator("llm_warm_up")
    @classmethod
    def checkWarmUp(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in WARM_UP_MODES:
            raise ValueError(f"llm_warm_up must be one of {WARM_UP_MODES}.")
        return value

    @field_validator("log_level")
    @classmethod
    def checkLogLevel(cls, value: str) -> str:
        value = value.strip().upper()
        if value not in logging.getLevelNamesMapping():
            raise ValueError(f"Unknown log_level {value!r}.")
        return value

    @property
    def frontendPath(self) -> str:
        path = os.path.expanduser(self.frontend_directory)
        return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)


# Error envelope shared by every error path so the frontend always receives the same JSON shape

def getRequestID(scope: Scope) -> str:
    return str(scope.get("state", {}).get("requestID", ""))


def errorResponse(status: int, code: str, message: str, requestID: str, detail: Any = None, headers: Optional[dict] = None) -> JSONResponse:
    error: dict[str, Any] = {"code": code, "message": message, "request_id": requestID}
    if detail is not None:
        error["detail"] = detail
    return JSONResponse({"error": error}, status_code=status, headers=headers)


def internalErrorResponse(scope: Scope, error: BaseException, debug: bool) -> JSONResponse:
    requestID = getRequestID(scope)
    # The traceback stays in the server log; clients only ever see the request ID to quote
    logger.error("Unhandled error on %s %s [%s]\n%s", scope.get("method"), scope.get("path"), requestID,
                 "".join(traceback.format_exception(error)))
    detail = {"type": type(error).__name__, "message": str(error)} if debug else None
    return errorResponse(500, "internal_error", "An internal server error occurred.", requestID, detail)


class ErrorEnvelopeMiddleware:
    # Sits inside CORSMiddleware, so even 500 responses carry the CORS headers the browser needs to read them
    def __init__(self, app: ASGIApp, debug: bool = False) -> None:
        self.app = app
        self.debug = debug

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        requestID = next(
            (value.decode("latin-1")[:64] for name, value in scope.get("headers", []) if name == b"x-request-id"),
            uuid.uuid4().hex,
        )
        scope.setdefault("state", {})["requestID"] = requestID
        responseStarted = False

        async def sendWithRequestID(message: Message) -> None:
            nonlocal responseStarted
            if message["type"] == "http.response.start":
                responseStarted = True
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = requestID
            await send(message)

        try:
            await self.app(scope, receive, sendWithRequestID)
        except Exception as error:
            if responseStarted:
                # Headers already went out; the connection can only be dropped
                raise
            await internalErrorResponse(scope, error, self.debug)(scope, receive, sendWithRequestID)


def registerExceptionHandlers(app: FastAPI, debug: bool) -> None:
    @app.exception_handler(HTTPException)
    async def handleHttpException(request: Request, error: HTTPException) -> JSONResponse:
        message = error.detail if isinstance(error.detail, str) else "Request failed."
        detail = None if isinstance(error.detail, str) else error.detail
        code = {401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed", 503: "service_unavailable"}.get(error.status_code, "http_error")
        return errorResponse(error.status_code, code, message, getRequestID(request.scope), detail, getattr(error, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def handleValidationError(request: Request, error: RequestValidationError) -> JSONResponse:
        # Field locations and messages only; raw input is left out so payloads are not echoed back
        fields = [{"location": list(item.get("loc", ())), "message": item.get("msg", ""), "type": item.get("type", "")} for item in error.errors()]
        return errorResponse(422, "validation_error", "Request validation failed.", getRequestID(request.scope), fields)

    @app.exception_handler(Exception)
    async def handleUnexpectedError(request: Request, error: Exception) -> JSONResponse:
        # Safety net in ServerErrorMiddleware, for errors raised outside ErrorEnvelopeMiddleware
        return internalErrorResponse(request.scope, error, debug)


# Static frontend serving with a client-side routing fallback

class SpaStaticFiles(StaticFiles):
    def __init__(self, directory: str, apiPrefix: str) -> None:
        super().__init__(directory=directory, html=True)
        self.indexPath = os.path.join(directory, "index.html")
        self.apiPrefix = apiPrefix

    def isApiPath(self, path: str) -> bool:
        return path == self.apiPrefix or path.startswith(self.apiPrefix + "/")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            # Unknown websocket paths end up on the root mount; refuse them instead of failing StaticFiles' assertion
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] == "http" and self.isApiPath(scope["path"]):
            raise HTTPException(status_code=404, detail=f"No API endpoint at {scope['path']}.")
        await super().__call__(scope, receive, send)

    async def get_response(self, path: str, scope: Scope) -> Any:
        try:
            return await super().get_response(path, scope)
        except HTTPException as error:
            # Extensionless paths like /dashboard/metrics belong to the client-side router; missing assets stay 404
            if error.status_code != 404 or os.path.splitext(path)[1] or not os.path.isfile(self.indexPath):
                raise
            return FileResponse(self.indexPath, headers={"Cache-Control": "no-cache"})


def mountFrontend(app: FastAPI, settings: ApiSettings) -> None:
    frontendPath = settings.frontendPath
    if os.path.isfile(os.path.join(frontendPath, "index.html")):
        app.mount("/", SpaStaticFiles(frontendPath, settings.api_prefix), name="frontend")
        app.state.frontendMounted = True
        return
    logger.warning("Frontend build not found at %s; serving the API only.", frontendPath)
    app.state.frontendMounted = False

    @app.get("/", include_in_schema=False)
    async def apiRoot() -> dict[str, Any]:
        return {"name": API_TITLE, "version": API_VERSION, "health": f"{settings.api_prefix}/health",
                "docs": "/docs" if settings.enable_docs else None}


# Dependencies shared with the router modules (e.g. `from api.main import getDatabaseManager`)

def getSettings(request: Request) -> ApiSettings:
    return request.app.state.settings


def getDatabaseManager(request: Request) -> DatabaseManager:
    databaseManager = getattr(request.app.state, "databaseManager", None)
    if databaseManager is None:
        raise HTTPException(status_code=503, detail="Database is unavailable; the API is running in degraded mode.")
    return databaseManager


def getLlmProvider(request: Request) -> LLMProvider:
    provider = getattr(request.app.state, "llmProvider", None)
    if provider is None:
        raise HTTPException(status_code=503, detail="LLM provider is not initialised.")
    return provider


def getGraphApp(request: Request) -> Any:
    graphApp = getattr(request.app.state, "graphApp", None)
    if graphApp is None:
        raise HTTPException(status_code=503, detail="Assessment workflow is not initialised.")
    return graphApp


async def verifyApiKey(request: Request) -> None:
    # No key configured means an open LAN deployment; once ICMIS_API_KEY is set every protected router needs it
    apiKey: Optional[SecretStr] = request.app.state.settings.api_key
    if apiKey is None or not apiKey.get_secret_value():
        return
    supplied = request.headers.get(API_KEY_HEADER, "")
    if not secrets.compare_digest(supplied.encode(), apiKey.get_secret_value().encode()):
        raise HTTPException(status_code=401, detail="Missing or invalid API key.", headers={"WWW-Authenticate": "ApiKey"})


# Built-in system router

def createSystemRouter() -> APIRouter:
    router = APIRouter(tags=["system"])

    @router.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        state = request.app.state
        databaseManager: Optional[DatabaseManager] = getattr(state, "databaseManager", None)
        schemaVersion: Optional[int] = None
        databaseStatus = "unavailable"
        if databaseManager is not None:
            try:
                rows = await asyncio.wait_for(databaseManager.fetchAll("SELECT MAX(version) AS version FROM schema_migrations;"), timeout=5.0)
                schemaVersion = int(rows[0]["version"] or 0) if rows else 0
                databaseStatus = "ok"
            except Exception as error:
                databaseStatus = f"error: {type(error).__name__}"
        startedAt: Optional[datetime] = getattr(state, "startedAt", None)
        return {
            "status": "ok" if databaseStatus == "ok" else "degraded",
            "version": API_VERSION,
            "database": databaseStatus,
            "schema_version": schemaVersion,
            "llm_warm_up": getattr(state, "warmUpStatus", "not started"),
            "routers_loaded": [name for name, status in state.routerStatus.items() if status == "loaded"],
            "routers_missing": [name for name, status in state.routerStatus.items() if status != "loaded"],
            "frontend_mounted": state.frontendMounted,
            "uptime_sec": round((datetime.now(timezone.utc) - startedAt).total_seconds(), 1) if startedAt else 0.0,
        }

    @router.get("/system/hardware", dependencies=[Depends(verifyApiKey)])
    async def hardware(provider: LLMProvider = Depends(getLlmProvider)) -> dict[str, Any]:
        status = await provider.assessEnvironment()
        return status.model_dump(mode="json")

    return router


# API router registration

def registerRouters(app: FastAPI, settings: ApiSettings) -> None:
    app.state.routerStatus = {}
    app.state.routerModules = []
    app.include_router(createSystemRouter(), prefix=settings.api_prefix)
    for entry in settings.router_modules:
        try:
            module = importlib.import_module(entry.module)
        except ModuleNotFoundError as error:
            # Only a missing route file is skipped; a broken import inside an existing one still stops startup
            if error.name != entry.module:
                raise
            logger.warning("Router module %s not found; skipping.", entry.module)
            app.state.routerStatus[entry.module] = "missing"
            continue
        router = getattr(module, "router", None)
        if not isinstance(router, APIRouter):
            raise TypeError(f"{entry.module} must define `router = APIRouter(...)`.")
        prefix = settings.api_prefix + ("/" + entry.prefix.strip("/") if entry.prefix.strip("/") else "")
        dependencies = [Depends(verifyApiKey)] if entry.require_auth else None
        app.include_router(router, prefix=prefix, tags=list(entry.tags) or None, dependencies=dependencies)
        app.state.routerModules.append(module)
        app.state.routerStatus[entry.module] = "loaded"


async def runHooks(app: FastAPI, hookName: str) -> None:
    for module in app.state.routerModules:
        hook = getattr(module, hookName, None)
        if hook is None:
            continue
        try:
            result = hook(app)
            if inspect.isawaitable(result):
                await result
        except Exception:
            if hookName == "onStartup":
                raise
            logger.exception("%s.%s failed.", module.__name__, hookName)


# Lifespan: persistent resources are ready before the first request and released on shutdown

async def warmUpLlm(app: FastAPI) -> None:
    settings: ApiSettings = app.state.settings
    app.state.warmUpStatus = "loading"
    warmUpStarted = time.monotonic()
    app.state.warmUpStatus = await app.state.llmProvider.warmUp(settings.llm_warm_up_timeout_sec)
    logger.info("LLM warm-up %s (%.1fs).", app.state.warmUpStatus, time.monotonic() - warmUpStarted)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings: ApiSettings = app.state.settings
    app.state.startedAt = datetime.now(timezone.utc)
    app.state.warmUpTask = None
    app.state.warmUpStatus = "off" if settings.llm_warm_up == "off" else "pending"

    # Startup phase
    databaseManager: Optional[DatabaseManager] = app.state.databaseManager or DatabaseManager()
    try:
        await databaseManager.start()
        logger.info("Database connected at %s.", databaseManager.databasePath)
    except Exception as error:
        try:
            await databaseManager.close()
        except Exception:
            pass
        if settings.require_database:
            raise
        logger.error("Database unavailable (%s: %s); running in degraded mode.", type(error).__name__, error)
        databaseManager = None
    app.state.databaseManager = databaseManager

    provider: LLMProvider = app.state.llmProvider or LLMProvider()
    app.state.llmProvider = provider
    app.state.graphApp = compileWorkflow(databaseManager, provider=provider)

    if settings.llm_warm_up == "blocking":
        await warmUpLlm(app)
    elif settings.llm_warm_up == "background":
        # The API accepts requests immediately; the first LLM call waits on the provider lock if loading is still running
        app.state.warmUpTask = asyncio.create_task(warmUpLlm(app))

    await runHooks(app, "onStartup")
    logger.info("%s %s ready on %s.", API_TITLE, API_VERSION, settings.api_prefix)
    try:
        yield
    finally:
        # Shutdown phase
        await runHooks(app, "onShutdown")
        warmUpTask: Optional[asyncio.Task] = app.state.warmUpTask
        if warmUpTask is not None and not warmUpTask.done():
            warmUpTask.cancel()
            await asyncio.gather(warmUpTask, return_exceptions=True)
        if app.state.databaseManager is not None:
            # close() drains the write queue, so pending telemetry is flushed to disk first
            await app.state.databaseManager.close()
            app.state.databaseManager = None
        if settings.unload_llm_on_shutdown:
            await provider.release()
        app.state.graphApp = None
        gc.collect()
        logger.info("%s shut down cleanly.", API_TITLE)


# Application instantiation

def createApp(
    settings: Optional[ApiSettings] = None,
    databaseManager: Optional[DatabaseManager] = None,
    provider: Optional[LLMProvider] = None,
) -> FastAPI:
    settings = settings or ApiSettings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if settings.enable_docs else None,
        redoc_url="/redoc" if settings.enable_docs else None,
        openapi_url=f"{settings.api_prefix}/openapi.json" if settings.enable_docs else None,
    )
    app.state.settings = settings
    app.state.databaseManager = databaseManager
    app.state.llmProvider = provider
    app.state.graphApp = None
    registerExceptionHandlers(app, settings.debug)

    # Middleware added last runs first: CORS wraps the error envelope so error responses keep CORS headers
    app.add_middleware(ErrorEnvelopeMiddleware, debug=settings.debug)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_origin_regex=settings.allowed_origin_regex,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
        expose_headers=[REQUEST_ID_HEADER],
    )

    # Routers go before the frontend mount so the "/" catch-all never shadows an API route
    registerRouters(app, settings)
    mountFrontend(app, settings)
    return app


app = createApp()


if __name__ == "__main__":
    import uvicorn

    runSettings: ApiSettings = app.state.settings
    # Reload needs an import string; otherwise the already-built app is served directly
    uvicorn.run(
        "api.main:app" if runSettings.debug else app,
        host=runSettings.host,
        port=runSettings.port,
        reload=runSettings.debug,
        log_level=runSettings.log_level.lower(),
        access_log=runSettings.debug,
    )