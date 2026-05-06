"""
Router de Impresos — endpoints ETL para datos de medios impresos
Arquitectura Event-Driven:
  POST /etl → Extract → S3 upload → (S3 Event Notification dispara SNS automáticamente)
            → EventStore registra DataIngested → retorna 202 Accepted
  DELETE    → S3 delete + DB delete → SNS publica DataDeleted → EventStore registra
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from sqlalchemy import func
from config.database import get_db, SessionLocal
from config.settings import get_settings
from config.auth import require_admin, require_reader, require_media_access
from config.limiter import limiter
from config.schemas import ETLRequest, ETLRangeRequest, FechaQuery
from models.api_auditsa_impresos import db_auditsa_impresos
from models.domain_events import DomainEvent
import services.stats_cache_service as stats_cache
from services.auditsa_impresos_etl_service import AuditsaImpresosEtlService
from services.auditsa_api_services import AuditsaApiService
from services.s3_service import S3Service
from services.sns_service import SNSService
from services.event_store_service import EventStoreService
from services.log_service import LogService

router = APIRouter(prefix="/api/impresos", tags=["Impresos"])
logger = logging.getLogger(__name__)

SNS_TOPIC = "impresos-events"
ETL_WORKERS = get_settings().etl_parallel_workers


@router.get("/stats", summary="Estadísticas generales de Impresos en base de datos")
async def get_stats(db: Session = Depends(get_db), _: dict = Depends(require_media_access("impresos"))):
    cached = stats_cache.get("impresos_stats")
    if cached:
        return cached
    result = db.query(
        func.count(db_auditsa_impresos.id).label("total_records"),
        func.count(func.distinct(db_auditsa_impresos.Fecha)).label("unique_dates"),
        func.count(func.distinct(db_auditsa_impresos.IdFuente)).label("unique_sources"),
        func.count(func.distinct(db_auditsa_impresos.Marca)).label("unique_brands"),
        func.count(func.distinct(db_auditsa_impresos.Anunciante)).label("unique_advertisers"),
        func.min(db_auditsa_impresos.Fecha).label("first_date"),
        func.max(db_auditsa_impresos.Fecha).label("last_date"),
        func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label("total_costo"),
    ).first()
    data = {
        "tipo": "impresos",
        "total_records": result.total_records or 0,
        "unique_dates": result.unique_dates or 0,
        "unique_sources": result.unique_sources or 0,
        "unique_brands": result.unique_brands or 0,
        "unique_advertisers": result.unique_advertisers or 0,
        "first_date": str(result.first_date) if result.first_date else None,
        "last_date": str(result.last_date) if result.last_date else None,
        "total_costo": float(result.total_costo or 0),
    }
    stats_cache.set("impresos_stats", data)
    return data


@router.post("/fix-nulls", summary="Limpia NULLs históricos en auditsa_api_impresos")
async def fix_nulls(db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    from services.auditsa_impresos_etl_service import AuditsaImpresosEtlService
    etl = AuditsaImpresosEtlService()
    etl._fix_nulls_in_db(db)
    return {"status": "ok", "message": "NULLs normalizados en auditsa_api_impresos"}


@router.get("/top-marcas", summary="Top marcas con mayor inversión en Impresos")
async def get_top_marcas(
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    cached = stats_cache.get("impresos_top_marcas")
    if cached:
        return cached
    rows = (
        db.query(
            db_auditsa_impresos.Anunciante,
            db_auditsa_impresos.Marca,
            func.count(db_auditsa_impresos.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label("total_costo"),
        )
        .filter(
            db_auditsa_impresos.Marca.notin_(['N.A.', '']),
        )
        .group_by(db_auditsa_impresos.Anunciante, db_auditsa_impresos.Marca)
        .order_by(func.sum(db_auditsa_impresos.Costo).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"anunciante": r.Anunciante, "marca": r.Marca, "registros": r.registros,
         "total_costo": float(r.total_costo), "total_inversion": float(r.total_costo)}
        for r in rows
    ]
    stats_cache.set("impresos_top_marcas", data)
    return data


@router.get("/top-sectores", summary="Top sectores de Impresos por inversión (todos los tiempos)")
async def get_top_sectores(
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    cached = stats_cache.get("impresos_top_sectores")
    if cached:
        return cached
    rows = (
        db.query(
            db_auditsa_impresos.Sector,
            func.count(db_auditsa_impresos.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label("total_inversion"),
        )
        .filter(db_auditsa_impresos.Sector.notin_(["N.A.", ""]))
        .group_by(db_auditsa_impresos.Sector)
        .order_by(func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"sector": r.Sector, "registros": r.registros, "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set("impresos_top_sectores", data)
    return data


@router.get("/records", summary="Registros de Impresos con filtros y paginación")
async def get_records(
    fecha_inicio: Optional[str] = Query(default=None, description="Fecha inicio YYYYMMDD"),
    fecha_fin: Optional[str] = Query(default=None, description="Fecha fin YYYYMMDD"),
    medio: Optional[str] = Query(default=None, description="Medios separados por coma"),
    fuente: Optional[str] = Query(default=None, description="Fuentes separadas por coma"),
    anunciante: Optional[str] = Query(default=None, description="Anunciantes separados por coma"),
    marca: Optional[str] = Query(default=None, description="Marcas separadas por coma"),
    sector: Optional[str] = Query(default=None, description="Sectores separados por coma"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    def parse_list(s: Optional[str]) -> List[str]:
        if not s:
            return []
        return [x.strip() for x in s.split(",") if x.strip()]

    q = db.query(db_auditsa_impresos)

    if fecha_inicio:
        q = q.filter(db_auditsa_impresos.Fecha >= datetime.strptime(fecha_inicio, "%Y%m%d").date())
    if fecha_fin:
        q = q.filter(db_auditsa_impresos.Fecha <= datetime.strptime(fecha_fin, "%Y%m%d").date())

    for values, col in [
        (parse_list(medio), db_auditsa_impresos.Medio),
        (parse_list(fuente), db_auditsa_impresos.Fuente),
        (parse_list(anunciante), db_auditsa_impresos.Anunciante),
        (parse_list(marca), db_auditsa_impresos.Marca),
        (parse_list(sector), db_auditsa_impresos.Sector),
    ]:
        if values:
            q = q.filter(col.in_(values))

    stats = q.with_entities(
        func.count(db_auditsa_impresos.id).label("total"),
        func.count(func.distinct(db_auditsa_impresos.Fecha)).label("unique_dates"),
        func.min(db_auditsa_impresos.Fecha).label("first_date"),
        func.max(db_auditsa_impresos.Fecha).label("last_date"),
        func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label("total_costo"),
        func.coalesce(func.avg(db_auditsa_impresos.Costo), 0).label("avg_costo"),
    ).first()

    medios_rows = (
        q.with_entities(
            db_auditsa_impresos.Medio,
            func.count(db_auditsa_impresos.id).label("cnt"),
        )
        .group_by(db_auditsa_impresos.Medio)
        .order_by(func.count(db_auditsa_impresos.id).desc())
        .limit(10)
        .all()
    )

    total = stats.total or 0
    records = (
        q.order_by(db_auditsa_impresos.Fecha.desc(), db_auditsa_impresos.id)
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    return {
        "summary": {
            "total_records": total,
            "unique_dates": stats.unique_dates or 0,
            "first_date": str(stats.first_date) if stats.first_date else None,
            "last_date": str(stats.last_date) if stats.last_date else None,
            "total_costo": float(stats.total_costo or 0),
            "avg_costo": float(stats.avg_costo or 0),
            "medios": [{"medio": r.Medio or "N/A", "count": r.cnt} for r in medios_rows],
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
                "Fuente": r.Fuente or "",
                "Medio": r.Medio or "",
                "Anunciante": r.Anunciante or "",
                "Marca": r.Marca or "",
                "Sector": r.Sector or "",
                "Categoria": r.Categoria or "",
                "Fecha": str(r.Fecha) if r.Fecha else "",
                "Costo": float(r.Costo or 0),
                "Tiraje": r.Tiraje or 0,
                "Dimension": float(r.Dimension or 0) if r.Dimension else 0,
            }
            for r in records
        ],
    }


@router.get("/{fecha}", summary="Obtener datos de Impresos desde Auditsa API")
async def get_data(
    fecha: str,
    limit: Optional[int] = Query(default=None, description="Límite de registros a devolver"),
    _: dict = Depends(require_media_access("impresos")),
):
    try:
        api = AuditsaApiService()
        data = api.get_impresos_data(fecha)
        if limit:
            data = data[:limit]
        return {"fecha": fecha, "total": len(data), "data": data}
    except Exception as e:
        logger.error(f"Error obteniendo datos Impresos para {fecha}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/range/{fecha_inicio}/{fecha_fin}", summary="Obtener datos de Impresos para un rango de fechas")
async def get_data_range(fecha_inicio: str, fecha_fin: str, _: dict = Depends(require_media_access("impresos"))):
    try:
        etl = AuditsaImpresosEtlService()
        fechas = etl._generate_date_range(fecha_inicio, fecha_fin)
        api = AuditsaApiService()
        all_data = []
        for fecha in fechas:
            data = api.get_impresos_data(fecha)
            all_data.extend(data)
        return {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin, "total": len(all_data), "data": all_data}
    except Exception as e:
        logger.error(f"Error obteniendo datos Impresos rango {fecha_inicio}-{fecha_fin}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etl", summary="Ejecutar ETL de Impresos para una fecha")
@limiter.limit("10/minute")
def run_etl(
    request: Request,
    data: ETLRequest,
    force: bool = Query(default=False, description="Forzar re-ingesta aunque ya existan datos"),
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    """
    Flujo EDA (idempotente):
    1. Verifica si ya existe un evento PENDING o datos procesados para la fecha
    2. Extract de Auditsa API
    3. Upload a S3 — la S3 Event Notification dispara automáticamente al topic SNS
       → SNS hace fan-out a impresos-spark-queue → Spark Consumer procesa asincrónamente
    4. Registra evento DataIngested en el Event Store (dedup automática)
    5. Retorna 202 Accepted con el event_id

    Si ya existen datos para la fecha, retorna 409 Conflict salvo que force=true.
    """
    etl = AuditsaImpresosEtlService()
    s3 = S3Service()
    es = EventStoreService(db)
    log = LogService(db)

    try:
        # Idempotencia: verificar si ya existen datos para esta fecha
        if not force:
            existing_count = db.query(func.count(db_auditsa_impresos.id)).filter(
                db_auditsa_impresos.Fecha == datetime.strptime(data.fecha, "%Y%m%d").date()
            ).scalar()
            if existing_count > 0:
                raise HTTPException(
                    status_code=409,
                    detail=f"Ya existen {existing_count} registros de Impresos para fecha={data.fecha}. "
                           f"Usa force=true para re-ingestar (Spark eliminará los datos previos automáticamente).",
                )

        raw_data = etl.extract_impresos_data(data.fecha)
        total = len(raw_data)

        s3_key = f"rawdata/impresos/{data.fecha}/data.json"
        s3_uri = f"s3://{s3.bucket}/{s3_key}"
        log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=total)
        s3.upload_json(s3_key, raw_data)  # dispara S3 Event Notification → SNS → SQS
        log.complete(log_id, total_records=total)

        event_id = es.publish(
            event_type="DataIngested",
            aggregate_type="impresos",
            aggregate_id=data.fecha,
            payload={"s3_key": s3_key, "s3_uri": s3_uri, "records": total, "fecha": data.fecha},
        )

        # Invalida caché de stats para que el próximo GET /stats recalcule desde DB
        stats_cache.invalidate("impresos_stats")
        stats_cache.invalidate("impresos_top_marcas")
        stats_cache.invalidate_prefix("imp_")

        return {
            "status": "accepted",
            "event_id": event_id,
            "fecha": data.fecha,
            "extracted_records": total,
            "s3_uri": s3_uri,
            "idempotent": True,
            "message": "Evento DataIngested registrado. S3 notificó a SNS → impresos-spark-queue. Spark Consumer procesará asincrónamente.",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL Impresos error para {data.fecha}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etl/range", summary="Ejecutar ETL de Impresos para un rango de fechas")
@limiter.limit("5/minute")
async def run_etl_range(
    request: Request,
    data: ETLRangeRequest,
    force: bool = Query(default=False, description="Forzar re-ingesta aunque ya existan datos"),
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaImpresosEtlService()
    s3 = S3Service()

    fechas = etl._generate_date_range(data.fecha_inicio, data.fecha_fin)

    def process_fecha(fecha: str) -> dict:
        thread_db = SessionLocal()
        try:
            # Idempotencia: verificar si ya existen datos para esta fecha
            if not force:
                existing_count = thread_db.query(func.count(db_auditsa_impresos.id)).filter(
                    db_auditsa_impresos.Fecha == datetime.strptime(fecha, "%Y%m%d").date()
                ).scalar()
                if existing_count > 0:
                    return {
                        "fecha": fecha,
                        "status": "skipped",
                        "records": existing_count,
                        "message": f"Ya existen {existing_count} registros. Usa force=true para re-ingestar.",
                    }

            log = LogService(thread_db)
            es = EventStoreService(thread_db)
            raw_data = etl.extract_impresos_data(fecha)
            total = len(raw_data)
            s3_key = f"rawdata/impresos/{fecha}/data.json"
            s3_uri = f"s3://{s3.bucket}/{s3_key}"

            log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=total)
            s3.upload_json(s3_key, raw_data)
            log.complete(log_id, total_records=total)

            event_id = es.publish(
                event_type="DataIngested",
                aggregate_type="impresos",
                aggregate_id=fecha,
                payload={"s3_key": s3_key, "s3_uri": s3_uri, "records": total, "fecha": fecha},
            )
            return {"fecha": fecha, "status": "accepted", "records": total, "event_id": event_id}
        except Exception as e:
            logger.error(f"ETL Impresos range error para {fecha}: {e}")
            return {"fecha": fecha, "status": "error", "error": str(e)}
        finally:
            thread_db.close()

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=ETL_WORKERS) as executor:
        results = list(await asyncio.gather(
            *[loop.run_in_executor(executor, process_fecha, f) for f in fechas]
        ))

    total_extracted = sum(r.get("records", 0) for r in results if r["status"] != "error")

    if total_extracted > 0:
        stats_cache.invalidate("impresos_stats")
        stats_cache.invalidate("impresos_top_marcas")
        stats_cache.invalidate_prefix("imp_")

    return {
        "status": "accepted",
        "fecha_inicio": data.fecha_inicio,
        "fecha_fin": data.fecha_fin,
        "total_fechas": len(fechas),
        "workers": ETL_WORKERS,
        "total_extracted_records": total_extracted,
        "dates": results,
        "message": f"Ingesta paralela ({ETL_WORKERS} workers). SNS notificó a impresos-spark-queue. Spark Consumer procesará asincrónamente.",
    }


@router.get("/status/range", summary="Estado de datos de Impresos para un rango con filtros")
async def get_status_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    def parse_csv(val):
        if not val: return []
        return [v.strip() for v in val.split(',') if v.strip()]

    fi = datetime.strptime(fecha_inicio, '%Y%m%d').date()
    ff = datetime.strptime(fecha_fin, '%Y%m%d').date()
    total_dates_in_range = (ff - fi).days + 1

    # Impresos no tiene Localidad, TipoMedio ni Industria — esos filtros se ignoran
    # (no devolvemos 0: el usuario puede tener otros filtros compatibles activos)

    # Caché: mismo rango + filtros → respuesta instantánea por TTL
    cache_key = stats_cache.make_key(
        "imp_status_range",
        fecha_inicio, fecha_fin,
        anunciante, marca, medio, categoria, sector_industria, submarca,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    q = db.query(
        func.count(db_auditsa_impresos.id).label('total_records'),
        func.count(func.distinct(db_auditsa_impresos.Fecha)).label('unique_dates'),
        func.count(func.distinct(db_auditsa_impresos.IdFuente)).label('unique_sources'),
        func.count(func.distinct(db_auditsa_impresos.Marca)).label('unique_brands'),
        func.count(func.distinct(db_auditsa_impresos.Anunciante)).label('unique_advertisers'),
        func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label('total_costo'),
    ).filter(
        db_auditsa_impresos.Fecha >= fi,
        db_auditsa_impresos.Fecha <= ff,
    )

    anunciante_list = parse_csv(anunciante)
    marca_list      = parse_csv(marca)
    medio_list      = parse_csv(medio)
    categoria_list  = parse_csv(categoria)

    if anunciante_list: q = q.filter(db_auditsa_impresos.Anunciante.in_(anunciante_list))
    if marca_list:      q = q.filter(db_auditsa_impresos.Marca.in_(marca_list))
    if medio_list:      q = q.filter(db_auditsa_impresos.Medio.in_(medio_list))
    if categoria_list:  q = q.filter(db_auditsa_impresos.Categoria.in_(categoria_list))
    sector_industria_list = parse_csv(sector_industria)
    if sector_industria_list: q = q.filter(db_auditsa_impresos.Sector.in_(sector_industria_list))
    submarca_list = parse_csv(submarca)
    if submarca_list: q = q.filter(db_auditsa_impresos.Submarca.in_(submarca_list))

    result = q.first()
    total_records = result.total_records or 0

    response = {
        "status": "ok",
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "total_dates_in_range": total_dates_in_range,
        "data_exists": total_records > 0,
        "statistics": {
            "total_records": total_records,
            "unique_dates": result.unique_dates or 0,
            "unique_sources": result.unique_sources or 0,
            "unique_brands": result.unique_brands or 0,
            "unique_advertisers": result.unique_advertisers or 0,
            "total_costo": float(result.total_costo or 0),
        },
    }
    stats_cache.set(cache_key, response)
    return response


@router.get("/top-marcas/range", summary="Top marcas de Impresos por inversión en un rango de fechas")
async def get_top_marcas_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    def parse_csv(val):
        if not val: return []
        return [v.strip() for v in val.split(',') if v.strip()]

    # Impresos no tiene Localidad, TipoMedio ni Industria — se ignoran, no se retorna vacío

    cache_key = stats_cache.make_key(
        "imp_top_marcas_range", fecha_inicio, fecha_fin, anunciante, marca, medio, categoria, sector_industria, submarca, limit
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    fi = datetime.strptime(fecha_inicio, '%Y%m%d').date()
    ff = datetime.strptime(fecha_fin, '%Y%m%d').date()

    q = db.query(
        db_auditsa_impresos.Anunciante,
        db_auditsa_impresos.Marca,
        func.count(db_auditsa_impresos.id).label('registros'),
        func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label('total_inversion'),
    ).filter(
        db_auditsa_impresos.Fecha >= fi,
        db_auditsa_impresos.Fecha <= ff,
        db_auditsa_impresos.Marca.notin_(['N.A.', '']),
    ).group_by(db_auditsa_impresos.Anunciante, db_auditsa_impresos.Marca)

    anunciante_list = parse_csv(anunciante)
    marca_list      = parse_csv(marca)
    medio_list      = parse_csv(medio)
    categoria_list  = parse_csv(categoria)

    if anunciante_list: q = q.filter(db_auditsa_impresos.Anunciante.in_(anunciante_list))
    if marca_list:      q = q.filter(db_auditsa_impresos.Marca.in_(marca_list))
    if medio_list:      q = q.filter(db_auditsa_impresos.Medio.in_(medio_list))
    if categoria_list:  q = q.filter(db_auditsa_impresos.Categoria.in_(categoria_list))
    sector_industria_list = parse_csv(sector_industria)
    if sector_industria_list: q = q.filter(db_auditsa_impresos.Sector.in_(sector_industria_list))
    submarca_list = parse_csv(submarca)
    if submarca_list: q = q.filter(db_auditsa_impresos.Submarca.in_(submarca_list))

    rows = q.order_by(func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).desc()).limit(limit).all()
    data = [
        {"anunciante": r.Anunciante, "marca": r.Marca, "registros": r.registros,
         "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/top-sectores/range", summary="Top sectores de Impresos por inversión en un rango de fechas")
async def get_top_sectores_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("impresos")),
):
    def parse_csv(val):
        if not val:
            return []
        return [v.strip() for v in val.split(",") if v.strip()]

    cache_key = stats_cache.make_key(
        "imp_top_sectores_range",
        fecha_inicio, fecha_fin,
        anunciante, marca, medio, categoria, sector_industria, submarca, limit,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    fi = datetime.strptime(fecha_inicio, "%Y%m%d").date()
    ff = datetime.strptime(fecha_fin, "%Y%m%d").date()

    q = (
        db.query(
            db_auditsa_impresos.Sector,
            func.count(db_auditsa_impresos.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).label("total_inversion"),
        )
        .filter(
            db_auditsa_impresos.Fecha >= fi,
            db_auditsa_impresos.Fecha <= ff,
            db_auditsa_impresos.Sector.notin_(["N.A.", ""]),
        )
        .group_by(db_auditsa_impresos.Sector)
    )
    for val, col in [
        (anunciante, db_auditsa_impresos.Anunciante),
        (marca, db_auditsa_impresos.Marca),
        (medio, db_auditsa_impresos.Medio),
        (categoria, db_auditsa_impresos.Categoria),
        (submarca, db_auditsa_impresos.Submarca),
    ]:
        vals = parse_csv(val)
        if vals:
            q = q.filter(col.in_(vals))
    si_list = parse_csv(sector_industria)
    if si_list:
        q = q.filter(db_auditsa_impresos.Sector.in_(si_list))

    rows = (
        q.order_by(func.coalesce(func.sum(db_auditsa_impresos.Costo), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"sector": r.Sector, "registros": r.registros, "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/status/{fecha}", summary="Estado de datos de Impresos para una fecha")
async def get_status(fecha: str, _: dict = Depends(require_media_access("impresos"))):
    etl = AuditsaImpresosEtlService()
    return etl.get_etl_status(fecha)


@router.delete("/range", summary="Eliminar datos de Impresos para un rango")
async def delete_data_range(data: ETLRangeRequest, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    etl = AuditsaImpresosEtlService()
    s3 = S3Service()
    sns = SNSService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        fechas = etl._generate_date_range(data.fecha_inicio, data.fecha_fin)
        s3_deleted = 0
        for f in fechas:
            s3_prefix = f"rawdata/impresos/{f}/"
            s3_uri = f"s3://{s3.bucket}/{s3_prefix}"
            log_s3 = log.start(source=s3_uri, instruction="DELETE", total_rows=0)
            count = s3.delete_prefix(s3_prefix)
            log.complete(log_s3, total_records=count)
            s3_deleted += count

        log_db = log.start(source="public.auditsa_api_impresos", instruction="DELETE", total_rows=0)
        db_result = etl.delete_impresos_data_range(data.fecha_inicio, data.fecha_fin, db)
        log.complete(log_db, total_records=db_result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="impresos",
            aggregate_id=f"{data.fecha_inicio}_{data.fecha_fin}",
            payload={"fecha_inicio": data.fecha_inicio, "fecha_fin": data.fecha_fin,
                     "s3_objects_deleted": s3_deleted, "db_records_deleted": db_result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete-range")
        sns.publish(SNS_TOPIC, "DataDeleted", {"tipo": "impresos", "fecha_inicio": data.fecha_inicio,
                                                "fecha_fin": data.fecha_fin, "event_id": event_id})

        return {**db_result, "s3_objects_deleted": s3_deleted, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{fecha}", summary="Eliminar datos de Impresos para una fecha")
async def delete_data(fecha: str, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    etl = AuditsaImpresosEtlService()
    s3 = S3Service()
    sns = SNSService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        s3_prefix = f"rawdata/impresos/{fecha}/"
        s3_uri = f"s3://{s3.bucket}/{s3_prefix}"
        log_s3 = log.start(source=s3_uri, instruction="DELETE", total_rows=0)
        s3_deleted = s3.delete_prefix(s3_prefix)
        log.complete(log_s3, total_records=s3_deleted)

        log_db = log.start(source="public.auditsa_api_impresos", instruction="DELETE", total_rows=0)
        db_result = etl.delete_impresos_data(fecha, db)
        log.complete(log_db, total_records=db_result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="impresos",
            aggregate_id=fecha,
            payload={"fecha": fecha, "s3_objects_deleted": s3_deleted, "db_records_deleted": db_result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete")
        sns.publish(SNS_TOPIC, "DataDeleted", {"tipo": "impresos", "fecha": fecha, "event_id": event_id})

        return {**db_result, "s3_objects_deleted": s3_deleted, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
