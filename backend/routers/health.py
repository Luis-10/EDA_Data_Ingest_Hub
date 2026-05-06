import time

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from config.database import sqlserver_engine
from config.settings import get_settings

router = APIRouter(prefix="/health", tags=["Health"])

_settings = get_settings()


@router.get("/sqlserver")
def health_sqlserver():
    """
    Verifica la conectividad con el servidor SQL Server externo.
    Retorna 200 si la conexión es exitosa, 503 si falla.
    """
    try:
        t0 = time.monotonic()
        with sqlserver_engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION AS version, @@SERVERNAME AS server_name"))
            row = result.fetchone()
        latency_ms = round((time.monotonic() - t0) * 1000, 1)

        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "host": _settings.sqlserver_host,
                "port": _settings.sqlserver_port,
                "database": _settings.sqlserver_db,
                "latency_ms": latency_ms,
                "server_name": row.server_name if row else None,
                "version": row.version.split("\n")[0] if row else None,
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "host": _settings.sqlserver_host,
                "port": _settings.sqlserver_port,
                "database": _settings.sqlserver_db,
                "detail": str(e),
            },
        )
