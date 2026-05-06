"""
Router de MediaMasterTotals — totales agregados del maestro de medios de Auditsa
Carga directa (sin S3/Spark): GetMediaMasterTotals API → Transform → PostgreSQL
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from config.auth import require_admin, require_reader
from config.database import get_db
from config.limiter import limiter
from config.schemas import ETLRangeRequest, FechaQuery
from models.api_auditsa_media_master_totals import db_auditsa_media_master_totals
from services.auditsa_api_services import AuditsaApiService
from services.auditsa_media_master_totals_etl_service import (
    AuditsaMediaMasterTotalsEtlService,
)
from services.s3_service import S3Service
from services.event_store_service import EventStoreService
from services.log_service import LogService
import services.stats_cache_service as stats_cache

router = APIRouter(prefix="/api/media-master-totals", tags=["Media Master Totals"])
logger = logging.getLogger(__name__)

_CACHE_PREFIX = "mmt_"


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_csv(val: Optional[str]):
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


def _parse_date(fecha: str):
    return datetime.strptime(fecha, "%Y%m%d").date()


# ─── Stats & Status ───────────────────────────────────────────────────────────

@router.get("/stats", summary="Estadísticas generales de MediaMasterTotals")
async def get_stats(
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cached = stats_cache.get("mmt_stats")
    if cached:
        return cached

    result = db.query(
        func.count(db_auditsa_media_master_totals.id).label("total_records"),
        func.count(func.distinct(db_auditsa_media_master_totals.IdMaster)).label("unique_masters"),
        func.count(func.distinct(db_auditsa_media_master_totals.Medio)).label("unique_medios"),
        func.count(func.distinct(db_auditsa_media_master_totals.Anunciante)).label("unique_advertisers"),
        func.count(func.distinct(db_auditsa_media_master_totals.Marca)).label("unique_brands"),
        func.min(db_auditsa_media_master_totals.FechaInicio).label("first_fecha"),
        func.max(db_auditsa_media_master_totals.FechaFin).label("last_fecha"),
        func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
    ).first()

    data = {
        "tipo": "media_master_totals",
        "total_records": result.total_records or 0,
        "unique_masters": result.unique_masters or 0,
        "unique_medios": result.unique_medios or 0,
        "unique_advertisers": result.unique_advertisers or 0,
        "unique_brands": result.unique_brands or 0,
        "first_fecha": str(result.first_fecha) if result.first_fecha else None,
        "last_fecha": str(result.last_fecha) if result.last_fecha else None,
        "total_inversion": float(result.total_inversion or 0),
    }
    stats_cache.set("mmt_stats", data)
    return data


@router.get("/status", summary="Estado y totales de la tabla auditsa_api_media_master_totals")
async def get_status(_: dict = Depends(require_reader)):
    return AuditsaMediaMasterTotalsEtlService().get_etl_status()


@router.get("/status/range", summary="Estado de MediaMasterTotals para un rango de fechas con filtros")
async def get_status_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    segmento: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    fi, ff = _parse_date(fecha_inicio), _parse_date(fecha_fin)

    cache_key = stats_cache.make_key(
        "mmt_status_range", fecha_inicio, fecha_fin,
        anunciante, marca, medio, industria, segmento, submarca,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    q = db.query(
        func.count(db_auditsa_media_master_totals.id).label("total_records"),
        func.count(func.distinct(db_auditsa_media_master_totals.IdMaster)).label("unique_masters"),
        func.count(func.distinct(db_auditsa_media_master_totals.Medio)).label("unique_medios"),
        func.count(func.distinct(db_auditsa_media_master_totals.Marca)).label("unique_brands"),
        func.count(func.distinct(db_auditsa_media_master_totals.Anunciante)).label("unique_advertisers"),
        func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
        func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
    ).filter(
        db_auditsa_media_master_totals.FechaInicio >= fi,
        db_auditsa_media_master_totals.FechaInicio <= ff,
    )

    for values, col in [
        (_parse_csv(anunciante), db_auditsa_media_master_totals.Anunciante),
        (_parse_csv(marca),      db_auditsa_media_master_totals.Marca),
        (_parse_csv(medio),      db_auditsa_media_master_totals.Medio),
        (_parse_csv(industria),  db_auditsa_media_master_totals.Industria),
        (_parse_csv(segmento),   db_auditsa_media_master_totals.Segmento),
        (_parse_csv(submarca),   db_auditsa_media_master_totals.Submarca),
    ]:
        if values:
            q = q.filter(col.in_(values))

    result = q.first()
    total_records = result.total_records or 0

    response = {
        "status": "ok",
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "data_exists": total_records > 0,
        "statistics": {
            "total_records": total_records,
            "unique_masters": result.unique_masters or 0,
            "unique_medios": result.unique_medios or 0,
            "unique_brands": result.unique_brands or 0,
            "unique_advertisers": result.unique_advertisers or 0,
            "total_inversion": float(result.total_inversion or 0),
            "total_hits": int(result.total_hits or 0),
        },
    }
    stats_cache.set(cache_key, response)
    return response


# ─── Top rankings ─────────────────────────────────────────────────────────────

@router.get("/top-marcas", summary="Top marcas de MediaMasterTotals por inversión (todos los tiempos)")
async def get_top_marcas(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cache_key = f"mmt_top_marcas_{limit}"
    cached = stats_cache.get(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            db_auditsa_media_master_totals.Anunciante,
            db_auditsa_media_master_totals.Marca,
            func.count(db_auditsa_media_master_totals.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
        )
        .filter(db_auditsa_media_master_totals.Marca.notin_(["N.A.", ""]))
        .group_by(
            db_auditsa_media_master_totals.Anunciante,
            db_auditsa_media_master_totals.Marca,
        )
        .order_by(func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {
            "anunciante": r.Anunciante,
            "marca": r.Marca,
            "registros": r.registros,
            "total_inversion": float(r.total_inversion or 0),
            "total_hits": int(r.total_hits or 0),
        }
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-marcas/range", summary="Top marcas de MediaMasterTotals por inversión en un rango de fechas")
async def get_top_marcas_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    segmento: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    fi, ff = _parse_date(fecha_inicio), _parse_date(fecha_fin)

    cache_key = stats_cache.make_key(
        "mmt_top_marcas_range", fecha_inicio, fecha_fin,
        anunciante, marca, medio, industria, segmento, submarca, limit,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    q = (
        db.query(
            db_auditsa_media_master_totals.Anunciante,
            db_auditsa_media_master_totals.Marca,
            func.count(db_auditsa_media_master_totals.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
        )
        .filter(
            db_auditsa_media_master_totals.FechaInicio >= fi,
            db_auditsa_media_master_totals.FechaInicio <= ff,
            db_auditsa_media_master_totals.Marca.notin_(["N.A.", ""]),
        )
        .group_by(
            db_auditsa_media_master_totals.Anunciante,
            db_auditsa_media_master_totals.Marca,
        )
    )

    for values, col in [
        (_parse_csv(anunciante), db_auditsa_media_master_totals.Anunciante),
        (_parse_csv(marca),      db_auditsa_media_master_totals.Marca),
        (_parse_csv(medio),      db_auditsa_media_master_totals.Medio),
        (_parse_csv(industria),  db_auditsa_media_master_totals.Industria),
        (_parse_csv(segmento),   db_auditsa_media_master_totals.Segmento),
        (_parse_csv(submarca),   db_auditsa_media_master_totals.Submarca),
    ]:
        if values:
            q = q.filter(col.in_(values))

    rows = q.order_by(
        func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).desc()
    ).limit(limit).all()

    data = [
        {
            "anunciante": r.Anunciante,
            "marca": r.Marca,
            "registros": r.registros,
            "total_inversion": float(r.total_inversion or 0),
            "total_hits": int(r.total_hits or 0),
        }
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-industrias", summary="Top industrias de MediaMasterTotals por inversión (todos los tiempos)")
async def get_top_industrias(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cache_key = f"mmt_top_industrias_{limit}"
    cached = stats_cache.get(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            db_auditsa_media_master_totals.Industria,
            func.count(db_auditsa_media_master_totals.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
        )
        .filter(db_auditsa_media_master_totals.Industria.notin_(["N.A.", ""]))
        .group_by(db_auditsa_media_master_totals.Industria)
        .order_by(func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {
            "industria": r.Industria,
            "registros": r.registros,
            "total_inversion": float(r.total_inversion or 0),
            "total_hits": int(r.total_hits or 0),
        }
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-industrias/range", summary="Top industrias de MediaMasterTotals por inversión en un rango de fechas")
async def get_top_industrias_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    segmento: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    fi, ff = _parse_date(fecha_inicio), _parse_date(fecha_fin)

    cache_key = stats_cache.make_key(
        "mmt_top_industrias_range", fecha_inicio, fecha_fin,
        anunciante, marca, medio, industria, segmento, submarca, limit,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    q = (
        db.query(
            db_auditsa_media_master_totals.Industria,
            func.count(db_auditsa_media_master_totals.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
        )
        .filter(
            db_auditsa_media_master_totals.FechaInicio >= fi,
            db_auditsa_media_master_totals.FechaInicio <= ff,
            db_auditsa_media_master_totals.Industria.notin_(["N.A.", ""]),
        )
        .group_by(db_auditsa_media_master_totals.Industria)
    )

    for values, col in [
        (_parse_csv(anunciante), db_auditsa_media_master_totals.Anunciante),
        (_parse_csv(marca),      db_auditsa_media_master_totals.Marca),
        (_parse_csv(medio),      db_auditsa_media_master_totals.Medio),
        (_parse_csv(industria),  db_auditsa_media_master_totals.Industria),
        (_parse_csv(segmento),   db_auditsa_media_master_totals.Segmento),
        (_parse_csv(submarca),   db_auditsa_media_master_totals.Submarca),
    ]:
        if values:
            q = q.filter(col.in_(values))

    rows = q.order_by(
        func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).desc()
    ).limit(limit).all()

    data = [
        {
            "industria": r.Industria,
            "registros": r.registros,
            "total_inversion": float(r.total_inversion or 0),
            "total_hits": int(r.total_hits or 0),
        }
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-medios", summary="Top medios de MediaMasterTotals por inversión (todos los tiempos)")
async def get_top_medios(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cache_key = f"mmt_top_medios_{limit}"
    cached = stats_cache.get(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            db_auditsa_media_master_totals.Medio,
            func.count(db_auditsa_media_master_totals.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).label("total_inversion"),
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalHit), 0).label("total_hits"),
        )
        .filter(db_auditsa_media_master_totals.Medio.notin_(["N.A.", ""]))
        .group_by(db_auditsa_media_master_totals.Medio)
        .order_by(func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {
            "medio": r.Medio,
            "registros": r.registros,
            "total_inversion": float(r.total_inversion or 0),
            "total_hits": int(r.total_hits or 0),
        }
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


# ─── Records ─────────────────────────────────────────────────────────────────

@router.get("/records", summary="Registros de totales con filtros y paginación")
async def get_records(
    medio: Optional[str] = Query(default=None, description="Medios separados por coma"),
    anunciante: Optional[str] = Query(default=None, description="Anunciantes separados por coma"),
    marca: Optional[str] = Query(default=None, description="Marcas separadas por coma"),
    industria: Optional[str] = Query(default=None, description="Industrias separadas por coma"),
    tipo_master: Optional[str] = Query(default=None, description="TipoMaster (ej. AUDIOVISUAL)"),
    fecha_inicio: Optional[str] = Query(default=None, description="FechaInicio YYYYMMDD"),
    fecha_fin: Optional[str] = Query(default=None, description="FechaFin YYYYMMDD"),
    search: Optional[str] = Query(default=None, description="Búsqueda libre en Version"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    q = db.query(db_auditsa_media_master_totals)

    if fecha_inicio:
        q = q.filter(db_auditsa_media_master_totals.FechaInicio >= _parse_date(fecha_inicio))
    if fecha_fin:
        q = q.filter(db_auditsa_media_master_totals.FechaFin <= _parse_date(fecha_fin))
    if tipo_master:
        q = q.filter(db_auditsa_media_master_totals.TipoMaster == tipo_master)
    if search:
        q = q.filter(db_auditsa_media_master_totals.Version.ilike(f"%{search}%"))
    for values, col in [
        (_parse_csv(medio),      db_auditsa_media_master_totals.Medio),
        (_parse_csv(anunciante), db_auditsa_media_master_totals.Anunciante),
        (_parse_csv(marca),      db_auditsa_media_master_totals.Marca),
        (_parse_csv(industria),  db_auditsa_media_master_totals.Industria),
    ]:
        if values:
            q = q.filter(col.in_(values))

    total = q.with_entities(func.count(db_auditsa_media_master_totals.id)).scalar() or 0
    total_inversion = (
        q.with_entities(
            func.coalesce(func.sum(db_auditsa_media_master_totals.TotalInversion), 0)
        ).scalar() or 0
    )

    records = (
        q.order_by(
            db_auditsa_media_master_totals.FechaInicio.desc(),
            db_auditsa_media_master_totals.IdMaster,
        )
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return {
        "summary": {
            "total_records": total,
            "total_inversion": float(total_inversion),
        },
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
        },
        "records": [
            {
                "id": r.id,
                "FechaInicio": str(r.FechaInicio) if r.FechaInicio else None,
                "FechaFin": str(r.FechaFin) if r.FechaFin else None,
                "Periodo": r.Periodo,
                "Medio": r.Medio,
                "Canal": r.Canal,
                "GEstacion": r.GEstacion,
                "FHoraria": r.FHoraria,
                "NoCorte": r.NoCorte,
                "TipoSpot": r.TipoSpot,
                "Genero": r.Genero,
                "TipoMaster": r.TipoMaster,
                "IdMaster": r.IdMaster,
                "Version": r.Version,
                "Anunciante": r.Anunciante,
                "Marca": r.Marca,
                "Submarca": r.Submarca,
                "Producto": r.Producto,
                "Segmento": r.Segmento,
                "Industria": r.Industria,
                "Mercado": r.Mercado,
                "DTeorica": r.DTeorica,
                "TotalHit": r.TotalHit,
                "TotalInversion": float(r.TotalInversion) if r.TotalInversion is not None else None,
                "Testigo": r.Testigo,
            }
            for r in records
        ],
    }


# ─── Raw API ──────────────────────────────────────────────────────────────────

@router.get(
    "/raw/{fecha_inicio}/{fecha_fin}",
    summary="Obtener datos crudos de la API GetMediaMasterTotals sin guardar",
)
async def get_raw(
    fecha_inicio: str,
    fecha_fin: str,
    limit: Optional[int] = Query(default=None, description="Límite de registros"),
    extra_param: Optional[str] = Query(default=None, description="Parámetro booleario adicional de la API"),
    _: dict = Depends(require_reader),
):
    try:
        api = AuditsaApiService()
        data = api.get_media_master_totals_data(fecha_inicio, fecha_fin, extra_param)
        if limit:
            data = data[:limit]
        return {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin, "total": len(data), "data": data}
    except Exception as e:
        logger.error(f"Error obteniendo raw MediaMasterTotals {fecha_inicio}-{fecha_fin}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── ETL ─────────────────────────────────────────────────────────────────────

@router.post("/etl", summary="Ejecutar ETL de MediaMasterTotals para un rango de fechas")
@limiter.limit("10/minute")
def run_etl(
    request: Request,
    data: ETLRangeRequest,
    force: bool = Query(
        default=False,
        description="Eliminar registros del mismo rango antes de insertar",
    ),
    extra_param: Optional[str] = Query(
        default=None,
        description="Parámetro booleario adicional de la API",
    ),
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    """
    Extrae GetMediaMasterTotals para el rango indicado, sube los datos crudos a S3,
    los carga directamente en PostgreSQL día a día y registra el evento en el Event Store.
    Con force=true elimina primero los registros cuyo (FechaInicio, FechaFin) coincide con el rango.
    """
    etl = AuditsaMediaMasterTotalsEtlService()
    s3 = S3Service()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        s3_key = (
            f"rawdata/media-master-totals/"
            f"{data.fecha_inicio}_{data.fecha_fin}/data.json"
        )
        s3_uri = f"s3://{s3.bucket}/{s3_key}"

        # Log RECORDING antes de iniciar el pipeline
        log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=0)

        if force:
            etl.delete_data_by_range(data.fecha_inicio, data.fecha_fin, db)

        # Pipeline extrae y carga día a día (una llamada a la API por día)
        result = etl.run_etl_pipeline(
            data.fecha_inicio, data.fecha_fin, extra_param, db
        )

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["message"])

        summary = result.get("etl_summary", {})
        loaded = summary.get("loaded_records", 0)

        # Subir resumen del ETL a S3 (sin re-extraer de la API)
        s3.upload_json(s3_key, {
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "extra_param": extra_param,
            "etl_summary": summary,
        })

        log.complete(log_id, total_records=loaded)

        event_id = es.publish(
            event_type="DataIngested",
            aggregate_type="media_master_totals",
            aggregate_id=f"{data.fecha_inicio}_{data.fecha_fin}",
            payload={
                "s3_key": s3_key,
                "s3_uri": s3_uri,
                "records": loaded,
                "fecha_inicio": data.fecha_inicio,
                "fecha_fin": data.fecha_fin,
            },
        )

        stats_cache.invalidate("mmt_stats")
        stats_cache.invalidate_prefix(_CACHE_PREFIX)

        return {
            "status": result["status"],
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "force": force,
            "s3_uri": s3_uri,
            "event_id": event_id,
            **summary,
            "message": result["message"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("ETL MediaMasterTotals error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Fix nulls ────────────────────────────────────────────────────────────────

@router.post("/fix-nulls", summary="Normalizar NULLs históricos en auditsa_api_media_master_totals")
async def fix_nulls(
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaMediaMasterTotalsEtlService()
    try:
        etl._fix_nulls_in_db(db)
        return {"status": "ok", "message": "NULLs normalizados en auditsa_api_media_master_totals"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Delete ───────────────────────────────────────────────────────────────────

@router.delete("/range", summary="Eliminar registros de un rango (FechaInicio, FechaFin)")
async def delete_by_range(
    data: ETLRangeRequest,
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaMediaMasterTotalsEtlService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        log_id = log.start(
            source="public.auditsa_api_media_master_totals",
            instruction="DELETE",
            total_rows=0,
        )
        result = etl.delete_data_by_range(data.fecha_inicio, data.fecha_fin, db)
        log.complete(log_id, total_records=result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="media_master_totals",
            aggregate_id=f"{data.fecha_inicio}_{data.fecha_fin}",
            payload={
                "fecha_inicio": data.fecha_inicio,
                "fecha_fin": data.fecha_fin,
                "records_deleted": result["records_deleted"],
            },
        )
        es.mark_processed(event_id, consumer_id="api-delete-range")

        stats_cache.invalidate("mmt_stats")
        stats_cache.invalidate_prefix(_CACHE_PREFIX)
        return {**result, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/all", summary="Vaciar completamente la tabla de MediaMasterTotals")
async def delete_all(
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaMediaMasterTotalsEtlService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        log_id = log.start(
            source="public.auditsa_api_media_master_totals",
            instruction="DELETE",
            total_rows=0,
        )
        result = etl.delete_all_data(db)
        log.complete(log_id, total_records=result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="media_master_totals",
            aggregate_id="all",
            payload={"records_deleted": result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete-all")

        stats_cache.invalidate("mmt_stats")
        stats_cache.invalidate_prefix(_CACHE_PREFIX)
        return {**result, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
