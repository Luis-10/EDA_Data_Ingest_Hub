import logging
import threading

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from config.auth import require_admin
from config.database import SqlServerBase, sqlserver_engine_tarifas, get_db
from config.settings import get_settings
import models.sqlserver_auditsa  # noqa: F401 — registra los modelos en SqlServerBase
import services.stats_cache_service as stats_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["Admin"])
_settings = get_settings()


@router.post(
    "/sqlserver/init-tables",
    summary="Crea las tablas de ingesta en SQL Server mxTarifas",
    dependencies=[Depends(require_admin)],
)
def init_sqlserver_tables():
    """
    Crea auditsa_api_tv, auditsa_api_radio y auditsa_api_impresos en mxTarifas.dbo.
    Es idempotente: si una tabla ya existe no la modifica.
    """
    try:
        SqlServerBase.metadata.create_all(bind=sqlserver_engine_tarifas, checkfirst=True)

        inspector = inspect(sqlserver_engine_tarifas)
        created = inspector.get_table_names(schema="dbo")
        target = [t.__tablename__ for t in [
            models.sqlserver_auditsa.ss_auditsa_tv,
            models.sqlserver_auditsa.ss_auditsa_radio,
            models.sqlserver_auditsa.ss_auditsa_impresos,
        ]]
        result = {t: ("ok" if t in created else "missing") for t in target}

        logger.info("SQL Server mxTarifas init-tables: %s", result)
        return {
            "status": "ok",
            "database": _settings.sqlserver_db_tarifas,
            "host": _settings.sqlserver_host,
            "tables": result,
        }
    except Exception as e:
        logger.error("Error creando tablas en SQL Server: %s", str(e))
        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(e)},
        )


@router.post(
    "/cache/refresh",
    summary="Invalida y recalcula el caché de stats (TV, Radio, Impresos)",
    dependencies=[Depends(require_admin)],
)
def refresh_stats_cache(db: Session = Depends(get_db)):
    """
    Limpia el caché de stats en memoria y recalcula los valores desde la DB.
    Útil cuando se ingesta nueva data y el caché aún no ha expirado (TTL = 10 min).
    """
    from sqlalchemy import func
    from models.api_auditsa_tv import db_auditsa_tv
    from models.api_auditsa_radio import db_auditsa_radio
    from models.api_auditsa_impresos import db_auditsa_impresos

    stats_cache.invalidate_all()
    refreshed = []

    try:
        r = db.query(
            func.count(db_auditsa_tv.id),
            func.count(func.distinct(db_auditsa_tv.Fecha)),
            func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0),
            func.min(db_auditsa_tv.Fecha),
            func.max(db_auditsa_tv.Fecha),
        ).first()
        stats_cache.set("tv_stats", {
            "tipo": "tv",
            "total_records": r[0] or 0,
            "unique_dates": r[1] or 0,
            "total_tarifa": float(r[2] or 0),
            "first_date": str(r[3]) if r[3] else None,
            "last_date": str(r[4]) if r[4] else None,
        })
        refreshed.append("tv_stats")
    except Exception as e:
        logger.warning("Cache refresh tv_stats error: %s", e)

    try:
        r = db.query(
            func.count(db_auditsa_radio.id),
            func.count(func.distinct(db_auditsa_radio.Fecha)),
            func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0),
            func.min(db_auditsa_radio.Fecha),
            func.max(db_auditsa_radio.Fecha),
        ).first()
        stats_cache.set("radio_stats", {
            "tipo": "radio",
            "total_records": r[0] or 0,
            "unique_dates": r[1] or 0,
            "total_tarifa": float(r[2] or 0),
            "first_date": str(r[3]) if r[3] else None,
            "last_date": str(r[4]) if r[4] else None,
        })
        refreshed.append("radio_stats")
    except Exception as e:
        logger.warning("Cache refresh radio_stats error: %s", e)

    try:
        r = db.query(
            func.count(db_auditsa_impresos.id),
            func.count(func.distinct(db_auditsa_impresos.Fecha)),
            func.coalesce(func.sum(db_auditsa_impresos.Costo), 0),
            func.min(db_auditsa_impresos.Fecha),
            func.max(db_auditsa_impresos.Fecha),
        ).first()
        stats_cache.set("impresos_stats", {
            "tipo": "impresos",
            "total_records": r[0] or 0,
            "unique_dates": r[1] or 0,
            "total_costo": float(r[2] or 0),
            "first_date": str(r[3]) if r[3] else None,
            "last_date": str(r[4]) if r[4] else None,
        })
        refreshed.append("impresos_stats")
    except Exception as e:
        logger.warning("Cache refresh impresos_stats error: %s", e)

    logger.info("Cache de stats refrescado manualmente: %s", refreshed)
    return {"status": "ok", "refreshed": refreshed}
