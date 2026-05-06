"""
Router de MediaMasterLibrary — catálogo de versiones/spots de Auditsa
Carga directa (sin S3/Spark): GetMediaMasterLibrary API → Transform → PostgreSQL
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from config.auth import require_admin, require_reader
from config.database import get_db
from config.limiter import limiter
from config.schemas import ETLRangeRequest, FechaQuery
from models.api_auditsa_media_master_library import db_auditsa_media_master_library
from services.auditsa_api_services import AuditsaApiService
from services.auditsa_media_master_library_etl_service import (
    AuditsaMediaMasterLibraryEtlService,
)
from services.s3_service import S3Service
from services.event_store_service import EventStoreService
from services.log_service import LogService
import services.stats_cache_service as stats_cache

router = APIRouter(prefix="/api/media-master-library", tags=["Media Master Library"])
logger = logging.getLogger(__name__)

_CACHE_PREFIX = "mml_"


# ─── Helpers ──────────────────────────────���───────────────────────────────────

def _parse_csv(val: Optional[str]):
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


# ─── Stats & Status ───────────────────────────────────────────────────────────

@router.get("/stats", summary="Estadísticas generales del catálogo de maestro de medios")
async def get_stats(
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cached = stats_cache.get("mml_stats")
    if cached:
        return cached

    result = db.query(
        func.count(db_auditsa_media_master_library.id).label("total_records"),
        func.count(func.distinct(db_auditsa_media_master_library.IdMaster)).label("unique_masters"),
        func.count(func.distinct(db_auditsa_media_master_library.TipoMaster)).label("unique_types"),
        func.count(func.distinct(db_auditsa_media_master_library.Anunciante)).label("unique_advertisers"),
        func.count(func.distinct(db_auditsa_media_master_library.Marca)).label("unique_brands"),
        func.count(func.distinct(db_auditsa_media_master_library.Industria)).label("unique_industries"),
    ).first()

    data = {
        "tipo": "media_master_library",
        "total_records": result.total_records or 0,
        "unique_masters": result.unique_masters or 0,
        "unique_types": result.unique_types or 0,
        "unique_advertisers": result.unique_advertisers or 0,
        "unique_brands": result.unique_brands or 0,
        "unique_industries": result.unique_industries or 0,
    }
    stats_cache.set("mml_stats", data)
    return data


@router.get("/status", summary="Estado y totales de la tabla auditsa_api_media_master_library")
async def get_status(_: dict = Depends(require_reader)):
    return AuditsaMediaMasterLibraryEtlService().get_etl_status()


# ─── Top rankings (catálogo, sin métrica de inversión → ranking por conteo) ───

@router.get("/top-anunciantes", summary="Top anunciantes del catálogo por número de versiones")
async def get_top_anunciantes(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cache_key = f"mml_top_anunciantes_{limit}"
    cached = stats_cache.get(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            db_auditsa_media_master_library.Anunciante,
            db_auditsa_media_master_library.Marca,
            func.count(db_auditsa_media_master_library.id).label("versiones"),
        )
        .filter(db_auditsa_media_master_library.Anunciante.notin_(["N.A.", ""]))
        .group_by(
            db_auditsa_media_master_library.Anunciante,
            db_auditsa_media_master_library.Marca,
        )
        .order_by(func.count(db_auditsa_media_master_library.id).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"anunciante": r.Anunciante, "marca": r.Marca, "versiones": r.versiones}
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-industrias", summary="Top industrias del catálogo por número de versiones")
async def get_top_industrias(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    cache_key = f"mml_top_industrias_{limit}"
    cached = stats_cache.get(cache_key)
    if cached:
        return cached

    rows = (
        db.query(
            db_auditsa_media_master_library.Industria,
            func.count(db_auditsa_media_master_library.id).label("versiones"),
        )
        .filter(db_auditsa_media_master_library.Industria.notin_(["N.A.", ""]))
        .group_by(db_auditsa_media_master_library.Industria)
        .order_by(func.count(db_auditsa_media_master_library.id).desc())
        .limit(limit)
        .all()
    )
    data = [{"industria": r.Industria, "versiones": r.versiones} for r in rows]
    stats_cache.set(cache_key, data)
    return data


# ─── Records ─────────────────────────────────────────────────────────────────

@router.get("/records", summary="Registros del catálogo con filtros y paginación")
async def get_records(
    tipo_master: Optional[str] = Query(default=None, description="Filtrar por TipoMaster"),
    anunciante: Optional[str] = Query(default=None, description="Anunciantes separados por coma"),
    marca: Optional[str] = Query(default=None, description="Marcas separadas por coma"),
    industria: Optional[str] = Query(default=None, description="Industrias separadas por coma"),
    segmento: Optional[str] = Query(default=None, description="Segmentos separados por coma"),
    mercado: Optional[str] = Query(default=None, description="Mercados separados por coma"),
    search: Optional[str] = Query(default=None, description="Búsqueda libre en Version"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    q = db.query(db_auditsa_media_master_library)

    if tipo_master:
        q = q.filter(db_auditsa_media_master_library.TipoMaster == tipo_master)
    if search:
        q = q.filter(db_auditsa_media_master_library.Version.ilike(f"%{search}%"))
    for values, col in [
        (_parse_csv(anunciante), db_auditsa_media_master_library.Anunciante),
        (_parse_csv(marca),      db_auditsa_media_master_library.Marca),
        (_parse_csv(industria),  db_auditsa_media_master_library.Industria),
        (_parse_csv(segmento),   db_auditsa_media_master_library.Segmento),
        (_parse_csv(mercado),    db_auditsa_media_master_library.Mercado),
    ]:
        if values:
            q = q.filter(col.in_(values))

    total = q.with_entities(func.count(db_auditsa_media_master_library.id)).scalar() or 0
    records = (
        q.order_by(db_auditsa_media_master_library.IdMaster)
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return {
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
        },
        "records": [
            {
                "id": r.id,
                "IdMaster": r.IdMaster,
                "TipoMaster": r.TipoMaster,
                "Version": r.Version,
                "Anunciante": r.Anunciante,
                "Marca": r.Marca,
                "Submarca": r.Submarca,
                "Producto": r.Producto,
                "Segmento": r.Segmento,
                "Industria": r.Industria,
                "Mercado": r.Mercado,
                "DTeorica": r.DTeorica,
                "Testigo": r.Testigo,
            }
            for r in records
        ],
    }


# ─── Raw API ─────────────────────────────────���────────────────────────────────

@router.get(
    "/raw/{fecha_inicio}/{fecha_fin}",
    summary="Obtener datos crudos de la API GetMediaMasterLibrary sin guardar",
)
async def get_raw(
    fecha_inicio: str,
    fecha_fin: str,
    limit: Optional[int] = Query(default=None, description="Límite de registros"),
    _: dict = Depends(require_reader),
):
    try:
        api = AuditsaApiService()
        data = api.get_media_master_library_data(fecha_inicio, fecha_fin)
        if limit:
            data = data[:limit]
        return {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin, "total": len(data), "data": data}
    except Exception as e:
        logger.error(f"Error obteniendo raw MediaMasterLibrary {fecha_inicio}-{fecha_fin}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── ETL ─────────────────────────────────────────────────────────────────────

@router.post("/etl", summary="Ejecutar ETL del catálogo para un rango de fechas")
@limiter.limit("10/minute")
def run_etl(
    request: Request,
    data: ETLRangeRequest,
    force: bool = Query(default=False, description="Vaciar tabla antes de insertar"),
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    """
    Extrae GetMediaMasterLibrary para el rango indicado, sube los datos crudos a S3,
    los carga directamente en PostgreSQL y registra el evento en el Event Store.
    Con force=true vacía la tabla completa antes de insertar (refresco total del catálogo).
    """
    etl = AuditsaMediaMasterLibraryEtlService()
    s3 = S3Service()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        raw_data = etl.extract_data(data.fecha_inicio, data.fecha_fin)
        total = len(raw_data)

        s3_key = f"rawdata/media-master-library/{data.fecha_inicio}_{data.fecha_fin}/data.json"
        s3_uri = f"s3://{s3.bucket}/{s3_key}"

        log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=total)
        s3.upload_json(s3_key, raw_data)

        if force:
            etl.delete_all_data(db)

        transformed_data = etl.transform_data(raw_data)
        load_result = etl.load_data(transformed_data, db)
        loaded = load_result["records_inserted"]

        log.complete(log_id, total_records=loaded)

        event_id = es.publish(
            event_type="DataIngested",
            aggregate_type="media_master_library",
            aggregate_id=f"{data.fecha_inicio}_{data.fecha_fin}",
            payload={
                "s3_key": s3_key,
                "s3_uri": s3_uri,
                "records": loaded,
                "fecha_inicio": data.fecha_inicio,
                "fecha_fin": data.fecha_fin,
            },
        )

        stats_cache.invalidate("mml_stats")
        stats_cache.invalidate_prefix(_CACHE_PREFIX)

        return {
            "status": "success",
            "fecha_inicio": data.fecha_inicio,
            "fecha_fin": data.fecha_fin,
            "force": force,
            "extracted_records": total,
            "transformed_records": len(transformed_data),
            "loaded_records": loaded,
            "s3_uri": s3_uri,
            "event_id": event_id,
            "message": f"ETL MediaMasterLibrary completado para {data.fecha_inicio} - {data.fecha_fin}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL MediaMasterLibrary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Fix nulls ────────────────────────────────────────────────────────────────

@router.post("/fix-nulls", summary="Normalizar NULLs históricos en auditsa_api_media_master_library")
async def fix_nulls(
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaMediaMasterLibraryEtlService()
    try:
        etl._fix_nulls_in_db(db)
        return {"status": "ok", "message": "NULLs normalizados en auditsa_api_media_master_library"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Delete ───────────────────────────────────────────────────────────────────

@router.delete("/all", summary="Vaciar completamente el catálogo de maestro de medios")
async def delete_all(
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaMediaMasterLibraryEtlService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        log_id = log.start(
            source="public.auditsa_api_media_master_library",
            instruction="DELETE",
            total_rows=0,
        )
        result = etl.delete_all_data(db)
        log.complete(log_id, total_records=result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="media_master_library",
            aggregate_id="all",
            payload={"records_deleted": result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete-all")

        stats_cache.invalidate("mml_stats")
        stats_cache.invalidate_prefix(_CACHE_PREFIX)
        return {**result, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
