"""
Router de Stats — endpoint batch que devuelve stats + top-marcas + top-sectores
de los 3 medios en una sola llamada, leyendo desde caché en memoria.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from config.database import get_db
from config.auth import require_reader
import services.stats_cache_service as stats_cache
from models.api_auditsa_tv import db_auditsa_tv
from models.api_auditsa_radio import db_auditsa_radio
from models.api_auditsa_impresos import db_auditsa_impresos

router = APIRouter(prefix="/api/stats", tags=["Stats"])

_TOP = 10
_EXCLUDED = ["N.A.", ""]


# ── Helpers por métrica (cada uno sólo corre su query si falta en caché) ──

def _tv_stats(db: Session) -> dict:
    r = db.query(
        func.count(db_auditsa_tv.id),
        func.count(func.distinct(db_auditsa_tv.Fecha)),
        func.count(func.distinct(db_auditsa_tv.IdCanal)),
        func.count(func.distinct(db_auditsa_tv.IdMarca)),
        func.count(func.distinct(db_auditsa_tv.IdAnunciante)),
        func.min(db_auditsa_tv.Fecha),
        func.max(db_auditsa_tv.Fecha),
        func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0),
    ).first()
    data = {
        "tipo": "tv", "total_records": r[0] or 0,
        "unique_dates": r[1] or 0, "unique_channels": r[2] or 0,
        "unique_brands": r[3] or 0, "unique_advertisers": r[4] or 0,
        "first_date": str(r[5]) if r[5] else None,
        "last_date": str(r[6]) if r[6] else None,
        "total_tarifa": float(r[7] or 0),
    }
    stats_cache.set("tv_stats", data)
    return data


def _tv_top_marcas(db: Session) -> list:
    rows = (
        db.query(db_auditsa_tv.Anunciante, db_auditsa_tv.Marca,
                 func.count(db_auditsa_tv.id),
                 func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0))
        .filter(db_auditsa_tv.Marca.notin_(_EXCLUDED),
                db_auditsa_tv.Marca.isnot(None))
        .group_by(db_auditsa_tv.Anunciante, db_auditsa_tv.Marca)
        .order_by(func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0).desc())
        .limit(_TOP).all()
    )
    data = [{"anunciante": m[0], "marca": m[1], "registros": m[2], "total_inversion": float(m[3])} for m in rows]
    stats_cache.set("tv_top_marcas", data)
    return data


def _tv_top_sectores(db: Session) -> list:
    rows = (
        db.query(db_auditsa_tv.Industria,
                 func.count(db_auditsa_tv.id),
                 func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0))
        .filter(db_auditsa_tv.Industria.notin_(_EXCLUDED),
                db_auditsa_tv.Industria.isnot(None))
        .group_by(db_auditsa_tv.Industria)
        .order_by(func.coalesce(func.sum(db_auditsa_tv.Tarifa), 0).desc())
        .limit(_TOP).all()
    )
    data = [{"sector": s[0], "registros": s[1], "total_inversion": float(s[2])} for s in rows]
    stats_cache.set("tv_top_sectores", data)
    return data


def _radio_stats(db: Session) -> dict:
    r = db.query(
        func.count(db_auditsa_radio.id),
        func.count(func.distinct(db_auditsa_radio.Fecha)),
        func.count(func.distinct(db_auditsa_radio.IdCanal)),
        func.count(func.distinct(db_auditsa_radio.IdMarca)),
        func.count(func.distinct(db_auditsa_radio.IdAnunciante)),
        func.min(db_auditsa_radio.Fecha),
        func.max(db_auditsa_radio.Fecha),
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0),
    ).first()
    data = {
        "tipo": "radio", "total_records": r[0] or 0,
        "unique_dates": r[1] or 0, "unique_channels": r[2] or 0,
        "unique_brands": r[3] or 0, "unique_advertisers": r[4] or 0,
        "first_date": str(r[5]) if r[5] else None,
        "last_date": str(r[6]) if r[6] else None,
        "total_tarifa": float(r[7] or 0),
    }
    stats_cache.set("radio_stats", data)
    return data


def _radio_top_marcas(db: Session) -> list:
    rows = (
        db.query(db_auditsa_radio.Anunciante, db_auditsa_radio.Marca,
                 func.count(db_auditsa_radio.id),
                 func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0))
        .filter(db_auditsa_radio.Marca.notin_(_EXCLUDED),
                db_auditsa_radio.Marca.isnot(None))
        .group_by(db_auditsa_radio.Anunciante, db_auditsa_radio.Marca)
        .order_by(func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc())
        .limit(_TOP).all()
    )
    data = [{"anunciante": m[0], "marca": m[1], "registros": m[2], "total_inversion": float(m[3])} for m in rows]
    stats_cache.set("radio_top_marcas", data)
    return data


def _radio_top_sectores(db: Session) -> list:
    rows = (
        db.query(db_auditsa_radio.Industria,
                 func.count(db_auditsa_radio.id),
                 func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0))
        .filter(db_auditsa_radio.Industria.notin_(_EXCLUDED),
                db_auditsa_radio.Industria.isnot(None))
        .group_by(db_auditsa_radio.Industria)
        .order_by(func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc())
        .limit(_TOP).all()
    )
    data = [{"sector": s[0], "registros": s[1], "total_inversion": float(s[2])} for s in rows]
    stats_cache.set("radio_top_sectores", data)
    return data


def _impresos_stats(db: Session) -> dict:
    r = db.query(
        func.count(db_auditsa_impresos.id),
        func.count(func.distinct(db_auditsa_impresos.Fecha)),
        func.count(func.distinct(db_auditsa_impresos.IdFuente)),
        func.count(func.distinct(db_auditsa_impresos.Marca)),
        func.count(func.distinct(db_auditsa_impresos.Anunciante)),
        func.min(db_auditsa_impresos.Fecha),
        func.max(db_auditsa_impresos.Fecha),
        func.coalesce(func.sum(db_auditsa_impresos.Costo), 0),
    ).first()
    data = {
        "tipo": "impresos", "total_records": r[0] or 0,
        "unique_dates": r[1] or 0, "unique_sources": r[2] or 0,
        "unique_brands": r[3] or 0, "unique_advertisers": r[4] or 0,
        "first_date": str(r[5]) if r[5] else None,
        "last_date": str(r[6]) if r[6] else None,
        "total_costo": float(r[7] or 0),
    }
    stats_cache.set("impresos_stats", data)
    return data


def _impresos_top_marcas(db: Session) -> list:
    rows = (
        db.query(db_auditsa_impresos.Anunciante, db_auditsa_impresos.Marca,
                 func.count(db_auditsa_impresos.id),
                 func.coalesce(func.sum(db_auditsa_impresos.Costo), 0))
        .filter(db_auditsa_impresos.Marca.notin_(_EXCLUDED),
                db_auditsa_impresos.Marca.isnot(None),
                db_auditsa_impresos.Anunciante.notin_(_EXCLUDED),
                db_auditsa_impresos.Anunciante.isnot(None))
        .group_by(db_auditsa_impresos.Anunciante, db_auditsa_impresos.Marca)
        .order_by(func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).desc())
        .limit(_TOP).all()
    )
    data = [
        {"anunciante": m[0], "marca": m[1], "registros": m[2],
         "total_inversion": float(m[3]), "total_costo": float(m[3])}
        for m in rows
    ]
    stats_cache.set("impresos_top_marcas", data)
    return data


def _impresos_top_sectores(db: Session) -> list:
    rows = (
        db.query(db_auditsa_impresos.Sector,
                 func.count(db_auditsa_impresos.id),
                 func.coalesce(func.sum(db_auditsa_impresos.Costo), 0))
        .filter(db_auditsa_impresos.Sector.notin_(_EXCLUDED),
                db_auditsa_impresos.Sector.isnot(None))
        .group_by(db_auditsa_impresos.Sector)
        .order_by(func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).desc())
        .limit(_TOP).all()
    )
    data = [{"sector": s[0], "registros": s[1], "total_inversion": float(s[2])} for s in rows]
    stats_cache.set("impresos_top_sectores", data)
    return data


@router.get("/summary", summary="Stats + top-marcas + top-sectores de los 3 medios en 1 llamada")
async def get_summary(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_reader),
):
    """
    Devuelve stats, top-10 marcas y top-10 sectores de cada medio en una sola respuesta.
    Cada métrica se lee de caché independientemente; solo computa las que faltan.
    Si tv_top_marcas está en caché pero tv_top_sectores no, solo corre la query de sectores.
    """
    allowed = set(current_user.get("allowed_media") or ["tv", "radio", "impresos"])
    if current_user.get("role") == "admin":
        allowed = {"tv", "radio", "impresos"}

    result: dict = {}

    if "tv" in allowed:
        result["tv"] = {
            "stats":        stats_cache.get("tv_stats")         or _tv_stats(db),
            "top_marcas":   stats_cache.get("tv_top_marcas")    or _tv_top_marcas(db),
            "top_sectores": stats_cache.get("tv_top_sectores")  or _tv_top_sectores(db),
        }

    if "radio" in allowed:
        result["radio"] = {
            "stats":        stats_cache.get("radio_stats")        or _radio_stats(db),
            "top_marcas":   stats_cache.get("radio_top_marcas")   or _radio_top_marcas(db),
            "top_sectores": stats_cache.get("radio_top_sectores") or _radio_top_sectores(db),
        }

    if "impresos" in allowed:
        result["impresos"] = {
            "stats":        stats_cache.get("impresos_stats")        or _impresos_stats(db),
            "top_marcas":   stats_cache.get("impresos_top_marcas")   or _impresos_top_marcas(db),
            "top_sectores": stats_cache.get("impresos_top_sectores") or _impresos_top_sectores(db),
        }

    return result
