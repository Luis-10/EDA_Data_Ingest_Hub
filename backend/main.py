import logging
import logging.config
import threading
import time
import traceback

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from config.database import Base, engine, SessionLocal, SqlServerBase, sqlserver_engine_tarifas, SqlServerSacBase, sqlserver_engine_sac
from config.limiter import limiter
from config.settings import get_settings
from routers import tv, radio, impresos, events, dlq, autocomplete, auth, export, records, health, admin, stats, media_master_library, media_master_totals
import services.stats_cache_service as stats_cache
from models.api_auditsa_tv import db_auditsa_tv
from models.api_auditsa_radio import db_auditsa_radio
from models.api_auditsa_impresos import db_auditsa_impresos

# sqlfunc: Funciones de SQL (MIN, MAX, COUNT, etc.)
from sqlalchemy import (
    func as sqlfunc,
)

# Importar modelos para que Base.metadata los registre en startup
import models.api_auditsa_tv  # noqa: F401
import models.api_auditsa_radio  # noqa: F401
import models.api_auditsa_impresos  # noqa: F401
import models.api_auditsa_logs  # noqa: F401
import models.api_auditsa_media_master_library  # noqa: F401
import models.api_auditsa_media_master_totals  # noqa: F401
import models.domain_events  # noqa: F401  ← Event Store
import models.catalogues_user_role  # noqa: F401  ← Gestión de usuarios PostgreSQL
import models.sqlserver_auditsa  # noqa: F401  ← SQL Server mxTarifas tables
import models.sqlserver_users  # noqa: F401  ← SQL Server user tables

#  Logging estructurado (JSON) 
_settings = get_settings()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
        "plain": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
    # Silenciar logs muy verbosos de SQLAlchemy en producción
    "loggers": {
        "sqlalchemy.engine": {"level": "WARNING"},
        "uvicorn.access": {"level": "INFO"},
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EDA Data Ingest Hub",
    version="2.0.0",
    description=(
        "API de ingesta de datos de medios publicitarios (TV, Radio, Impresos) "
        "con Arquitectura Event-Driven. "
        "Flujo: Auditsa API → S3 → SNS (Event Bus) → SQS → Spark → PostgreSQL. "
        "El Event Store registra cada evento de dominio como entidad inmutable.\n\n"
        "**Autenticación**: obtén un token en `POST /auth/token` y úsalo como Bearer."
    ),
)

#  Manejo centralizado de errores 
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.warning('{"event":"http_error","status":%d,"path":"%s","detail":"%s"}',
                   exc.status_code, request.url.path, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status_code": exc.status_code,
            "detail": exc.detail,
            "path": str(request.url.path),
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    logger.warning('{"event":"validation_error","path":"%s","errors":%s}',
                   request.url.path, errors)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": True,
            "status_code": 422,
            "detail": "Error de validación en los parámetros de la solicitud.",
            "errors": [
                {"field": " → ".join(str(l) for l in e["loc"]), "msg": e["msg"]}
                for e in errors
            ],
            "path": str(request.url.path),
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Loguea el traceback completo pero no lo expone al cliente
    logger.error(
        '{"event":"unhandled_exception","path":"%s","exc_type":"%s","detail":"%s"}',
        request.url.path, type(exc).__name__, str(exc),
        exc_info=True,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": True,
            "status_code": 500,
            "detail": "Error interno del servidor. Contacta al administrador.",
            "path": str(request.url.path),
        },
    )


# Rate limiting (slowapi)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Exponer métricas en /metrics para Prometheus
Instrumentator().instrument(app).expose(app)


_INDEXES = [
    # Habilitar extensión trigrama (necesaria para GIN ILIKE rápido)
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
    # Fecha — usada en MIN/MAX/COUNT(DISTINCT)/WHERE BETWEEN en las 3 tablas (B-tree)
    'CREATE INDEX IF NOT EXISTS idx_tv_fecha        ON public.auditsa_api_tv       ("Fecha")',
    'CREATE INDEX IF NOT EXISTS idx_radio_fecha     ON public.auditsa_api_radio    ("Fecha")',
    'CREATE INDEX IF NOT EXISTS idx_impresos_fecha  ON public.auditsa_api_impresos ("Fecha")',
    # Índices compuestos para top-marcas/range (Fecha + Marca + Anunciante + Tarifa/Costo)
    # Permiten que la query de GROUP BY + ORDER BY SUM(Tarifa) sea un Index-Only Scan
    'CREATE INDEX IF NOT EXISTS idx_tv_top_marcas_range    ON public.auditsa_api_tv       ("Fecha", "Marca", "Anunciante", "Tarifa")',
    'CREATE INDEX IF NOT EXISTS idx_radio_top_marcas_range ON public.auditsa_api_radio    ("Fecha", "Marca", "Anunciante", "Tarifa")',
    'CREATE INDEX IF NOT EXISTS idx_imp_top_marcas_range   ON public.auditsa_api_impresos ("Fecha", "Marca", "Anunciante", "Costo")',
    # Autocomplete TV: GIN trigrama para ILIKE '%q%' rápido
    'CREATE INDEX IF NOT EXISTS idx_tv_anunciante_trgm  ON public.auditsa_api_tv    USING GIN ("Anunciante" gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_tv_marca_trgm       ON public.auditsa_api_tv    USING GIN ("Marca"       gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_tv_localidad_trgm   ON public.auditsa_api_tv    USING GIN ("Localidad"   gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_tv_tipomedio_trgm   ON public.auditsa_api_tv    USING GIN ("TipoMedio"   gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_tv_medio_trgm       ON public.auditsa_api_tv    USING GIN ("Medio"       gin_trgm_ops)',
    # Autocomplete Radio: GIN trigrama
    'CREATE INDEX IF NOT EXISTS idx_radio_anunciante_trgm ON public.auditsa_api_radio USING GIN ("Anunciante" gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_marca_trgm      ON public.auditsa_api_radio USING GIN ("Marca"      gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_localidad_trgm  ON public.auditsa_api_radio USING GIN ("Localidad"  gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_tipomedio_trgm  ON public.auditsa_api_radio USING GIN ("TipoMedio"  gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_medio_trgm      ON public.auditsa_api_radio USING GIN ("Medio"      gin_trgm_ops)',
    # Autocomplete Impresos: GIN trigrama
    'CREATE INDEX IF NOT EXISTS idx_imp_anunciante_trgm ON public.auditsa_api_impresos USING GIN ("Anunciante" gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_marca_trgm      ON public.auditsa_api_impresos USING GIN ("Marca"      gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_medio_trgm      ON public.auditsa_api_impresos USING GIN ("Medio"      gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_sector_trgm     ON public.auditsa_api_impresos USING GIN ("Sector"     gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_categoria_trgm  ON public.auditsa_api_impresos USING GIN ("Categoria"  gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_fuente_trgm     ON public.auditsa_api_impresos USING GIN ("Fuente"     gin_trgm_ops)',
    # Autocomplete: Submarca en las 3 tablas + Industria en TV y Radio
    'CREATE INDEX IF NOT EXISTS idx_tv_submarca_trgm    ON public.auditsa_api_tv       USING GIN ("Submarca"    gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_submarca_trgm ON public.auditsa_api_radio    USING GIN ("Submarca"    gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_tv_industria_trgm    ON public.auditsa_api_tv      USING GIN ("Industria"   gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_radio_industria_trgm ON public.auditsa_api_radio   USING GIN ("Industria"   gin_trgm_ops)',
    'CREATE INDEX IF NOT EXISTS idx_imp_submarca_trgm   ON public.auditsa_api_impresos USING GIN ("Submarca"    gin_trgm_ops)',
    # B-tree en Industria para loose index scan (DISTINCT sobre columnas de baja cardinalidad)
    'CREATE INDEX IF NOT EXISTS idx_tv_industria_btree    ON public.auditsa_api_tv    ("Industria")',
    'CREATE INDEX IF NOT EXISTS idx_radio_industria_btree ON public.auditsa_api_radio ("Industria")',
    #  Idempotencia: índice parcial único en domain_events 
    # Previene que existan múltiples eventos PENDING para el mismo (event_type, aggregate_type, aggregate_id).
    # Solo aplica a eventos con status='PENDING'; los PROCESSED/FAILED pueden repetirse (ej: re-ingesta con force).
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_domain_events_pending_dedup
       ON public.domain_events (event_type, aggregate_type, aggregate_id)
       WHERE status = 'PENDING'""",
]


def _create_indexes():
    # Crea índices de rendimiento para mejorar consultas frecuentes.
    try:
        with engine.connect() as conn:
            for stmt in _INDEXES:
                conn.execute(text(stmt))
            conn.commit()
        logger.info("Índices de rendimiento verificados/creados.")
    except Exception as e:
        logger.error(f"Error creando índices en background: {e}")


def _prewarm_stats():
    """Pre-calcula y cachea los stats de las 3 tablas al inicio.
    TV y Radio tienen ~20M y ~15M filas — cada query tarda ~40s sin caché.
    Al pre-calentar en background, el primer request del usuario es instantáneo.
    """
    db = SessionLocal()
    try:
        logger.info("Pre-calentando caché de stats (TV, Radio, Impresos)...")

        r = db.query(
            sqlfunc.count(db_auditsa_tv.id),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_tv.Fecha)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_tv.IdCanal)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_tv.IdMarca)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_tv.IdAnunciante)),
            sqlfunc.min(db_auditsa_tv.Fecha),
            sqlfunc.max(db_auditsa_tv.Fecha),
            sqlfunc.coalesce(sqlfunc.sum(db_auditsa_tv.Tarifa), 0),
        ).first()
        stats_cache.set(
            "tv_stats",
            {
                "tipo": "tv",
                "total_records": r[0] or 0,
                "unique_dates": r[1] or 0,
                "unique_channels": r[2] or 0,
                "unique_brands": r[3] or 0,
                "unique_advertisers": r[4] or 0,
                "first_date": str(r[5]) if r[5] else None,
                "last_date": str(r[6]) if r[6] else None,
                "total_tarifa": float(r[7] or 0),
            },
        )
        logger.info("Caché TV stats listo.")

        r = db.query(
            sqlfunc.count(db_auditsa_radio.id),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_radio.Fecha)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_radio.IdCanal)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_radio.IdMarca)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_radio.IdAnunciante)),
            sqlfunc.min(db_auditsa_radio.Fecha),
            sqlfunc.max(db_auditsa_radio.Fecha),
            sqlfunc.coalesce(sqlfunc.sum(db_auditsa_radio.Tarifa), 0),
        ).first()
        stats_cache.set(
            "radio_stats",
            {
                "tipo": "radio",
                "total_records": r[0] or 0,
                "unique_dates": r[1] or 0,
                "unique_channels": r[2] or 0,
                "unique_brands": r[3] or 0,
                "unique_advertisers": r[4] or 0,
                "first_date": str(r[5]) if r[5] else None,
                "last_date": str(r[6]) if r[6] else None,
                "total_tarifa": float(r[7] or 0),
            },
        )
        logger.info("Caché Radio stats listo.")

        r = db.query(
            sqlfunc.count(db_auditsa_impresos.id),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_impresos.Fecha)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_impresos.IdFuente)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_impresos.Marca)),
            sqlfunc.count(sqlfunc.distinct(db_auditsa_impresos.Anunciante)),
            sqlfunc.min(db_auditsa_impresos.Fecha),
            sqlfunc.max(db_auditsa_impresos.Fecha),
            sqlfunc.coalesce(sqlfunc.sum(db_auditsa_impresos.Costo), 0),
        ).first()
        stats_cache.set(
            "impresos_stats",
            {
                "tipo": "impresos",
                "total_records": r[0] or 0,
                "unique_dates": r[1] or 0,
                "unique_sources": r[2] or 0,
                "unique_brands": r[3] or 0,
                "unique_advertisers": r[4] or 0,
                "first_date": str(r[5]) if r[5] else None,
                "last_date": str(r[6]) if r[6] else None,
                "total_costo": float(r[7] or 0),
            },
        )

        top_imp = (
            db.query(
                db_auditsa_impresos.Anunciante,
                db_auditsa_impresos.Marca,
                sqlfunc.count(db_auditsa_impresos.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_impresos.Costo), 0),
            )
            .filter(
                db_auditsa_impresos.Anunciante.notin_(["N.A.", "", None]),
                db_auditsa_impresos.Marca.notin_(["N.A.", "", None]),
            )
            .group_by(db_auditsa_impresos.Anunciante, db_auditsa_impresos.Marca)
            .order_by(sqlfunc.sum(db_auditsa_impresos.Costo).desc())
            .limit(5)
            .all()
        )
        stats_cache.set(
            "impresos_top_marcas",
            [
                {
                    "anunciante": row[0],
                    "marca": row[1],
                    "registros": row[2],
                    "total_costo": float(row[3]),
                    "total_inversion": float(row[3]),
                }
                for row in top_imp
            ],
        )
        logger.info("Caché Impresos stats + top-marcas listo.")

        top_tv = (
            db.query(
                db_auditsa_tv.Anunciante,
                db_auditsa_tv.Marca,
                sqlfunc.count(db_auditsa_tv.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_tv.Tarifa), 0),
            )
            .filter(
                db_auditsa_tv.Anunciante.notin_(["N.A.", "", None]),
                db_auditsa_tv.Marca.notin_(["N.A.", "", None]),
            )
            .group_by(db_auditsa_tv.Anunciante, db_auditsa_tv.Marca)
            .order_by(sqlfunc.sum(db_auditsa_tv.Tarifa).desc())
            .limit(5)
            .all()
        )
        stats_cache.set(
            "tv_top_marcas",
            [
                {
                    "anunciante": row[0],
                    "marca": row[1],
                    "registros": row[2],
                    "total_inversion": float(row[3]),
                }
                for row in top_tv
            ],
        )
        logger.info("Caché TV top-marcas listo.")

        top_radio = (
            db.query(
                db_auditsa_radio.Anunciante,
                db_auditsa_radio.Marca,
                sqlfunc.count(db_auditsa_radio.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_radio.Tarifa), 0),
            )
            .filter(
                db_auditsa_radio.Anunciante.notin_(["N.A.", "", None]),
                db_auditsa_radio.Marca.notin_(["N.A.", "", None]),
            )
            .group_by(db_auditsa_radio.Anunciante, db_auditsa_radio.Marca)
            .order_by(sqlfunc.sum(db_auditsa_radio.Tarifa).desc())
            .limit(5)
            .all()
        )
        stats_cache.set(
            "radio_top_marcas",
            [
                {
                    "anunciante": row[0],
                    "marca": row[1],
                    "registros": row[2],
                    "total_inversion": float(row[3]),
                }
                for row in top_radio
            ],
        )
        logger.info("Caché Radio top-marcas listo.")

        top_tv_sec = (
            db.query(
                db_auditsa_tv.Industria,
                sqlfunc.count(db_auditsa_tv.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_tv.Tarifa), 0),
            )
            .filter(db_auditsa_tv.Industria.notin_(["N.A.", "", None]))
            .group_by(db_auditsa_tv.Industria)
            .order_by(sqlfunc.sum(db_auditsa_tv.Tarifa).desc())
            .limit(10).all()
        )
        stats_cache.set("tv_top_sectores", [
            {"sector": row[0], "registros": row[1], "total_inversion": float(row[2])}
            for row in top_tv_sec
        ])
        logger.info("Caché TV top-sectores listo.")

        top_radio_sec = (
            db.query(
                db_auditsa_radio.Industria,
                sqlfunc.count(db_auditsa_radio.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_radio.Tarifa), 0),
            )
            .filter(db_auditsa_radio.Industria.notin_(["N.A.", "", None]))
            .group_by(db_auditsa_radio.Industria)
            .order_by(sqlfunc.sum(db_auditsa_radio.Tarifa).desc())
            .limit(10).all()
        )
        stats_cache.set("radio_top_sectores", [
            {"sector": row[0], "registros": row[1], "total_inversion": float(row[2])}
            for row in top_radio_sec
        ])
        logger.info("Caché Radio top-sectores listo.")

        top_imp_sec = (
            db.query(
                db_auditsa_impresos.Sector,
                sqlfunc.count(db_auditsa_impresos.id),
                sqlfunc.coalesce(sqlfunc.sum(db_auditsa_impresos.Costo), 0),
            )
            .filter(db_auditsa_impresos.Sector.notin_(["N.A.", "", None]))
            .group_by(db_auditsa_impresos.Sector)
            .order_by(sqlfunc.sum(db_auditsa_impresos.Costo).desc())
            .limit(10).all()
        )
        stats_cache.set("impresos_top_sectores", [
            {"sector": row[0], "registros": row[1], "total_inversion": float(row[2])}
            for row in top_imp_sec
        ])
        logger.info("Caché Impresos top-sectores listo.")

    except Exception as e:
        logger.error(f"Error pre-calentando caché de stats: {e}")
    finally:
        db.close()


def _background_init():
    _create_indexes()
    _prewarm_stats()


@app.on_event("startup")
def on_startup():
    try:
        logger.info("Verificando base de datos PostgreSQL...")
        Base.metadata.create_all(bind=engine)
        logger.info("PostgreSQL lista → tablas verificadas/creadas.")
    except Exception as e:
        logger.error(f"Error al conectar a PostgreSQL o crear tablas: {e}")

    try:
        logger.info("Verificando base de datos SQL Server mxTarifas...")
        SqlServerBase.metadata.create_all(bind=sqlserver_engine_tarifas, checkfirst=True)
        logger.info("SQL Server mxTarifas lista → tablas verificadas/creadas.")
    except Exception as e:
        logger.warning(f"SQL Server mxTarifas no disponible en startup (dual-write seguirá intentando): {e}")

    try:
        logger.info("Verificando base de datos SQL Server appSAC (catálogos)...")
        with sqlserver_engine_sac.connect() as conn:
            conn.execute(text("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'app') EXEC('CREATE SCHEMA app')"))
            conn.commit()
        SqlServerSacBase.metadata.create_all(bind=sqlserver_engine_sac, checkfirst=True)
        logger.info("SQL Server appSAC lista → tablas verificadas/creadas.")
    except Exception as e:
        logger.warning(f"SQL Server appSAC no disponible en startup (dual-write seguirá intentando): {e}")

    # Crear índices y pre-calentar caché en background (en segundo plano).
    threading.Thread(target=_background_init, daemon=True).start()


# Registrar routers
app.include_router(auth.router)
app.include_router(tv.router)
app.include_router(radio.router)
app.include_router(impresos.router)
app.include_router(events.router)
app.include_router(dlq.router)
app.include_router(autocomplete.router)
app.include_router(export.router)
app.include_router(records.router)
app.include_router(health.router)
app.include_router(admin.router)
app.include_router(stats.router)
app.include_router(media_master_library.router)
app.include_router(media_master_totals.router)


@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "EDA Data Ingest Hub",
        "version": "2.0.0",
        "architecture": "Event-Driven (S3 → SNS → SQS → Spark)",
        "docs": "/docs",
        "metrics": "/metrics",
        "endpoints": {
            "tv": "/api/tv",
            "radio": "/api/radio",
            "impresos": "/api/impresos",
            "events": "/api/events",
            "dlq": "/api/dlq",
            "media_master_library": "/api/media-master-library",
            "media_master_totals": "/api/media-master-totals",
        },
    }


@app.get("/health", tags=["Root"])
async def health():
    """
    Health check completo: verifica PostgreSQL, S3 y SQS.
    Retorna 200 si todo está disponible, 503 si algún componente falla.
    """
    checks: dict = {}
    healthy = True

    # PostgreSQL
    try:
        t0 = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["postgresql"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 1)}
    except Exception as e:
        checks["postgresql"] = {"status": "error", "detail": str(e)}
        healthy = False

    # S3 (LocalStack / AWS)
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        s3 = boto3.client(
            "s3",
            endpoint_url=_settings.aws_endpoint_url,
            aws_access_key_id=_settings.aws_access_key_id,
            aws_secret_access_key=_settings.aws_secret_access_key,
            region_name=_settings.aws_region,
            config=BotoConfig(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1}),
        )
        t0 = time.monotonic()
        s3.head_bucket(Bucket=_settings.s3_bucket)
        checks["s3"] = {"status": "ok", "bucket": _settings.s3_bucket, "latency_ms": round((time.monotonic() - t0) * 1000, 1)}
    except Exception as e:
        checks["s3"] = {"status": "error", "detail": str(e)}
        healthy = False

    # SQS
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        sqs = boto3.client(
            "sqs",
            endpoint_url=_settings.aws_endpoint_url,
            aws_access_key_id=_settings.aws_access_key_id,
            aws_secret_access_key=_settings.aws_secret_access_key,
            region_name=_settings.aws_region,
            config=BotoConfig(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1}),
        )
        t0 = time.monotonic()
        sqs.list_queues(MaxResults=1)
        checks["sqs"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 1)}
    except Exception as e:
        checks["sqs"] = {"status": "error", "detail": str(e)}
        healthy = False

    payload = {"status": "healthy" if healthy else "degraded", "checks": checks}
    return JSONResponse(
        status_code=200 if healthy else 503,
        content=payload,
    )
