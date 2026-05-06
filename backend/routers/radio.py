"""
Router de Radio — endpoints ETL para datos de radio
Arquitectura Event-Driven:
  POST /etl → Extract → S3 upload → (S3 Event Notification dispara SNS automáticamente)
            → EventStore registra DataIngested → retorna 202 Accepted
  DELETE    → S3 delete + DB delete → SNS publica DataDeleted → EventStore registra
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from sqlalchemy import func
from config.database import get_db, SessionLocal
from config.settings import get_settings
from config.auth import require_admin, require_reader, require_media_access
from config.limiter import limiter
from config.schemas import ETLRequest, ETLRangeRequest, FechaQuery
from models.api_auditsa_radio import db_auditsa_radio
from models.domain_events import DomainEvent
import services.stats_cache_service as stats_cache
from services.auditsa_radio_etl_service import AuditsaRadioEtlService
from services.auditsa_api_services import AuditsaApiService
from services.s3_service import S3Service
from services.sns_service import SNSService
from services.event_store_service import EventStoreService
from services.log_service import LogService

router = APIRouter(prefix="/api/radio", tags=["Radio"])
logger = logging.getLogger(__name__)

SNS_TOPIC = "radio-events"
ETL_WORKERS = get_settings().etl_parallel_workers


@router.get("/stats", summary="Estadísticas generales de Radio en base de datos")
async def get_stats(db: Session = Depends(get_db), _: dict = Depends(require_media_access("radio"))):
    cached = stats_cache.get("radio_stats")
    if cached:
        return cached
    result = db.query(
        func.count(db_auditsa_radio.id).label("total_records"),
        func.count(func.distinct(db_auditsa_radio.Fecha)).label("unique_dates"),
        func.count(func.distinct(db_auditsa_radio.IdCanal)).label("unique_channels"),
        func.count(func.distinct(db_auditsa_radio.IdMarca)).label("unique_brands"),
        func.count(func.distinct(db_auditsa_radio.IdAnunciante)).label("unique_advertisers"),
        func.min(db_auditsa_radio.Fecha).label("first_date"),
        func.max(db_auditsa_radio.Fecha).label("last_date"),
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label("total_tarifa"),
    ).first()
    data = {
        "tipo": "radio",
        "total_records": result.total_records or 0,
        "unique_dates": result.unique_dates or 0,
        "unique_channels": result.unique_channels or 0,
        "unique_brands": result.unique_brands or 0,
        "unique_advertisers": result.unique_advertisers or 0,
        "first_date": str(result.first_date) if result.first_date else None,
        "last_date": str(result.last_date) if result.last_date else None,
        "total_tarifa": float(result.total_tarifa or 0),
    }
    stats_cache.set("radio_stats", data)
    return data


@router.get("/top-marcas", summary="Top marcas de Radio por inversión (todos los tiempos)")
async def get_top_marcas(
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("radio")),
):
    cached = stats_cache.get("radio_top_marcas")
    if cached:
        return cached
    rows = db.query(
        db_auditsa_radio.Anunciante,
        db_auditsa_radio.Marca,
        func.count(db_auditsa_radio.id).label('registros'),
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label('total_inversion'),
    ).filter(
        db_auditsa_radio.Marca.notin_(['N.A.', '']),    # not_in_: excluir marcas no válidas.
    ).group_by(
        db_auditsa_radio.Anunciante, db_auditsa_radio.Marca,
    ).order_by(
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc()
    ).limit(limit).all()
    data = [
        {"anunciante": r.Anunciante, "marca": r.Marca, "registros": r.registros,
         "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set("radio_top_marcas", data)
    return data


@router.get(
    "/top-sectores",
    summary="Top sectores de Radio por inversión (todos los tiempos)",
)
async def get_top_sectores(
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("radio")),
):
    cached = stats_cache.get("radio_top_sectores")
    if cached:
        return cached
    rows = (
        db.query(
            db_auditsa_radio.Industria,
            func.count(db_auditsa_radio.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label("total_inversion"),
        )
        .filter(db_auditsa_radio.Industria.notin_(["N.A.", ""]))
        .group_by(db_auditsa_radio.Industria)
        .order_by(func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"sector": r.Industria, "registros": r.registros, "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set("radio_top_sectores", data)
    return data


@router.get("/{fecha}", summary="Obtener datos de Radio desde Auditsa API")
async def get_data(
    fecha: str,
    limit: Optional[int] = Query(default=None, description="Límite de registros a devolver"),
    _: dict = Depends(require_media_access("radio")),
):
    try:
        api = AuditsaApiService()
        etl = AuditsaRadioEtlService()
        all_data = []
        for radio_id in etl.radio_ids:
            data = api.get_radio_data(fecha, radio_id)
            all_data.extend(data)
        if limit:
            all_data = all_data[:limit]
        return {"fecha": fecha, "total": len(all_data), "data": all_data}
    except Exception as e:
        logger.error(f"Error obteniendo datos Radio para {fecha}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/range/{fecha_inicio}/{fecha_fin}", summary="Obtener datos de Radio para un rango de fechas")
async def get_data_range(fecha_inicio: str, fecha_fin: str, _: dict = Depends(require_media_access("radio"))):
    try:
        etl = AuditsaRadioEtlService()
        fechas = etl._generate_date_range(fecha_inicio, fecha_fin)
        api = AuditsaApiService()
        all_data = []
        for fecha in fechas:
            for radio_id in etl.radio_ids:
                data = api.get_radio_data(fecha, radio_id)
                all_data.extend(data)
        return {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin, "total": len(all_data), "data": all_data}
    except Exception as e:
        logger.error(f"Error obteniendo datos Radio rango {fecha_inicio}-{fecha_fin}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etl", summary="Ejecutar ETL de Radio para una fecha")
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
       → SNS hace fan-out a radio-spark-queue → Spark Consumer procesa asincrónamente
    4. Registra evento DataIngested en el Event Store (dedup automática)
    5. Retorna 202 Accepted con el event_id

    Si ya existen datos para la fecha, retorna 409 Conflict salvo que force=true.
    """
    etl = AuditsaRadioEtlService()
    s3 = S3Service()
    es = EventStoreService(db)
    log = LogService(db)

    try:
        # Idempotencia: verificar si ya existen datos para esta fecha
        if not force:
            existing_count = db.query(func.count(db_auditsa_radio.id)).filter(
                db_auditsa_radio.Fecha == datetime.strptime(data.fecha, "%Y%m%d").date()
            ).scalar()
            if existing_count > 0:
                raise HTTPException(
                    status_code=409,
                    detail=f"Ya existen {existing_count} registros de Radio para fecha={data.fecha}. "
                           f"Usa force=true para re-ingestar (Spark eliminará los datos previos automáticamente).",
                )

        raw_data = etl.extract_radio_data(data.fecha)
        total = len(raw_data)

        s3_key = f"rawdata/radio/{data.fecha}/data.json"
        s3_uri = f"s3://{s3.bucket}/{s3_key}"
        log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=total)
        s3.upload_json(s3_key, raw_data)  # dispara S3 Event Notification → SNS → SQS
        log.complete(log_id, total_records=total)

        event_id = es.publish(
            event_type="DataIngested",
            aggregate_type="radio",
            aggregate_id=data.fecha,
            payload={"s3_key": s3_key, "s3_uri": s3_uri, "records": total, "fecha": data.fecha},
        )

        return {
            "status": "accepted",
            "event_id": event_id,
            "fecha": data.fecha,
            "extracted_records": total,
            "s3_uri": s3_uri,
            "idempotent": True,
            "message": "Evento DataIngested registrado. S3 notificó a SNS → radio-spark-queue. Spark Consumer procesará asincrónamente.",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL Radio error para {data.fecha}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/etl/range", summary="Ejecutar ETL de Radio para un rango de fechas")
@limiter.limit("5/minute")
async def run_etl_range(
    request: Request,
    data: ETLRangeRequest,
    force: bool = Query(default=False, description="Forzar re-ingesta aunque ya existan datos"),
    db: Session = Depends(get_db),
    _: dict = Depends(require_admin),
):
    etl = AuditsaRadioEtlService()
    s3 = S3Service()

    fechas = etl._generate_date_range(data.fecha_inicio, data.fecha_fin)

    def process_fecha(fecha: str) -> dict:
        thread_db = SessionLocal()
        try:
            # Idempotencia: verificar si ya existen datos para esta fecha
            if not force:
                existing_count = thread_db.query(func.count(db_auditsa_radio.id)).filter(
                    db_auditsa_radio.Fecha == datetime.strptime(fecha, "%Y%m%d").date()
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
            raw_data = etl.extract_radio_data(fecha)
            total = len(raw_data)
            s3_key = f"rawdata/radio/{fecha}/data.json"
            s3_uri = f"s3://{s3.bucket}/{s3_key}"

            log_id = log.start(source=s3_uri, instruction="INSERT", total_rows=total)
            s3.upload_json(s3_key, raw_data)
            log.complete(log_id, total_records=total)

            event_id = es.publish(
                event_type="DataIngested",
                aggregate_type="radio",
                aggregate_id=fecha,
                payload={"s3_key": s3_key, "s3_uri": s3_uri, "records": total, "fecha": fecha},
            )
            return {"fecha": fecha, "status": "accepted", "records": total, "event_id": event_id}
        except Exception as e:
            logger.error(f"ETL Radio range error para {fecha}: {e}")
            return {"fecha": fecha, "status": "error", "error": str(e)}
        finally:
            thread_db.close()

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=ETL_WORKERS) as executor:
        results = list(await asyncio.gather(
            *[loop.run_in_executor(executor, process_fecha, f) for f in fechas]
        ))

    total_extracted = sum(r.get("records", 0) for r in results if r["status"] != "error")
    return {
        "status": "accepted",
        "fecha_inicio": data.fecha_inicio,
        "fecha_fin": data.fecha_fin,
        "total_fechas": len(fechas),
        "workers": ETL_WORKERS,
        "total_extracted_records": total_extracted,
        "dates": results,
        "message": f"Ingesta paralela ({ETL_WORKERS} workers). SNS notificó a radio-spark-queue. Spark Consumer procesará asincrónamente.",
    }


@router.get("/status/range", summary="Estado de datos de Radio para un rango con filtros")
async def get_status_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("radio")),
):
    def parse_csv(val):
        if not val: return []
        return [v.strip() for v in val.split(',') if v.strip()]

    fi = datetime.strptime(fecha_inicio, '%Y%m%d').date()
    ff = datetime.strptime(fecha_fin, '%Y%m%d').date()
    total_dates_in_range = (ff - fi).days + 1

    # Radio no tiene Categoria; si ese filtro está activo → sin datos
    if parse_csv(categoria):
        return {
            "status": "ok",
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "total_dates_in_range": total_dates_in_range,
            "data_exists": False,
            "statistics": {"total_records": 0, "unique_dates": 0, "unique_channels": 0,
                           "unique_brands": 0, "unique_advertisers": 0},
        }

    # Caché: mismo rango + filtros → respuesta instantánea por TTL
    cache_key = stats_cache.make_key(
        "radio_status_range",
        fecha_inicio, fecha_fin,
        anunciante, marca, medio, tipo_medio, localidad,
        sector_industria, industria, submarca,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    q = db.query(
        func.count(db_auditsa_radio.id).label('total_records'),
        func.count(func.distinct(db_auditsa_radio.Fecha)).label('unique_dates'),
        func.count(func.distinct(db_auditsa_radio.IdCanal)).label('unique_channels'),
        func.count(func.distinct(db_auditsa_radio.IdMarca)).label('unique_brands'),
        func.count(func.distinct(db_auditsa_radio.IdAnunciante)).label('unique_advertisers'),
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label('total_tarifa'),
    ).filter(
        db_auditsa_radio.Fecha >= fi,
        db_auditsa_radio.Fecha <= ff,
    )

    anunciante_list = parse_csv(anunciante)
    marca_list      = parse_csv(marca)
    medio_list      = parse_csv(medio)
    tipo_medio_list = parse_csv(tipo_medio)
    localidad_list  = parse_csv(localidad)

    if anunciante_list: q = q.filter(db_auditsa_radio.Anunciante.in_(anunciante_list))
    if marca_list:      q = q.filter(db_auditsa_radio.Marca.in_(marca_list))
    if medio_list:      q = q.filter(db_auditsa_radio.Medio.in_(medio_list))
    if tipo_medio_list: q = q.filter(db_auditsa_radio.TipoMedio.in_(tipo_medio_list))
    if localidad_list:  q = q.filter(db_auditsa_radio.Localidad.in_(localidad_list))
    sector_industria_list = parse_csv(sector_industria)
    if sector_industria_list: q = q.filter(db_auditsa_radio.Industria.in_(sector_industria_list))
    industria_list = parse_csv(industria)
    if industria_list: q = q.filter(db_auditsa_radio.Industria.in_(industria_list))
    submarca_list = parse_csv(submarca)
    if submarca_list: q = q.filter(db_auditsa_radio.Submarca.in_(submarca_list))

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
            "unique_channels": result.unique_channels or 0,
            "unique_brands": result.unique_brands or 0,
            "unique_advertisers": result.unique_advertisers or 0,
            "total_tarifa": float(result.total_tarifa or 0),
        },
    }
    stats_cache.set(cache_key, response)
    return response


@router.get("/top-marcas/range", summary="Top marcas de Radio por inversión en un rango de fechas")
async def get_top_marcas_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("radio")),
):
    def parse_csv(val):
        if not val: return []
        return [v.strip() for v in val.split(',') if v.strip()]

    if parse_csv(categoria):
        return []

    cache_key = stats_cache.make_key(
        "radio_top_marcas_range", fecha_inicio, fecha_fin, anunciante, marca, medio, tipo_medio, localidad, sector_industria, industria, submarca, limit
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    fi = datetime.strptime(fecha_inicio, '%Y%m%d').date()
    ff = datetime.strptime(fecha_fin, '%Y%m%d').date()

    q = db.query(
        db_auditsa_radio.Anunciante,
        db_auditsa_radio.Marca,
        func.count(db_auditsa_radio.id).label('registros'),
        func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label('total_inversion'),
    ).filter(
        db_auditsa_radio.Fecha >= fi,
        db_auditsa_radio.Fecha <= ff,
        db_auditsa_radio.Marca.notin_(['N.A.', '']),
    ).group_by(db_auditsa_radio.Anunciante, db_auditsa_radio.Marca)

    anunciante_list = parse_csv(anunciante)
    marca_list      = parse_csv(marca)
    medio_list      = parse_csv(medio)
    tipo_medio_list = parse_csv(tipo_medio)
    localidad_list  = parse_csv(localidad)

    if anunciante_list: q = q.filter(db_auditsa_radio.Anunciante.in_(anunciante_list))
    if marca_list:      q = q.filter(db_auditsa_radio.Marca.in_(marca_list))
    if medio_list:      q = q.filter(db_auditsa_radio.Medio.in_(medio_list))
    if tipo_medio_list: q = q.filter(db_auditsa_radio.TipoMedio.in_(tipo_medio_list))
    if localidad_list:  q = q.filter(db_auditsa_radio.Localidad.in_(localidad_list))
    sector_industria_list = parse_csv(sector_industria)
    if sector_industria_list: q = q.filter(db_auditsa_radio.Industria.in_(sector_industria_list))
    industria_list = parse_csv(industria)
    if industria_list: q = q.filter(db_auditsa_radio.Industria.in_(industria_list))
    submarca_list = parse_csv(submarca)
    if submarca_list: q = q.filter(db_auditsa_radio.Submarca.in_(submarca_list))

    rows = q.order_by(func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc()).limit(limit).all()
    data = [
        {"anunciante": r.Anunciante, "marca": r.Marca, "registros": r.registros,
         "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get(
    "/top-sectores/range",
    summary="Top sectores de Radio por inversión en un rango de fechas",
)
async def get_top_sectores_range(
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=20),
    db: Session = Depends(get_db),
    _: dict = Depends(require_media_access("radio")),
):
    def parse_csv(val):
        if not val:
            return []
        return [v.strip() for v in val.split(",") if v.strip()]

    cache_key = stats_cache.make_key(
        "radio_top_sectores_range",
        fecha_inicio, fecha_fin,
        anunciante, marca, medio, tipo_medio, localidad,
        industria, sector_industria, submarca, limit,
    )
    cached = stats_cache.get(cache_key)
    if cached is not None:
        return cached

    fi = datetime.strptime(fecha_inicio, "%Y%m%d").date()
    ff = datetime.strptime(fecha_fin, "%Y%m%d").date()

    q = (
        db.query(
            db_auditsa_radio.Industria,
            func.count(db_auditsa_radio.id).label("registros"),
            func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).label("total_inversion"),
        )
        .filter(
            db_auditsa_radio.Fecha >= fi,
            db_auditsa_radio.Fecha <= ff,
            db_auditsa_radio.Industria.notin_(["N.A.", ""]),
        )
        .group_by(db_auditsa_radio.Industria)
    )
    for val, col in [
        (anunciante, db_auditsa_radio.Anunciante),
        (marca, db_auditsa_radio.Marca),
        (medio, db_auditsa_radio.Medio),
        (tipo_medio, db_auditsa_radio.TipoMedio),
        (localidad, db_auditsa_radio.Localidad),
        (submarca, db_auditsa_radio.Submarca),
        (industria, db_auditsa_radio.Industria),
        (sector_industria, db_auditsa_radio.Industria),
    ]:
        vals = parse_csv(val)
        if vals:
            q = q.filter(col.in_(vals))

    rows = (
        q.order_by(func.coalesce(func.sum(db_auditsa_radio.Tarifa), 0).desc())
        .limit(limit)
        .all()
    )
    data = [
        {"sector": r.Industria, "registros": r.registros, "total_inversion": float(r.total_inversion or 0)}
        for r in rows
    ]
    stats_cache.set(cache_key, data)
    return data


@router.get("/status/{fecha}", summary="Estado de datos de Radio para una fecha")
async def get_status(fecha: str, _: dict = Depends(require_media_access("radio"))):
    etl = AuditsaRadioEtlService()
    return etl.get_etl_status(fecha)


@router.delete("/range", summary="Eliminar datos de Radio para un rango")
async def delete_data_range(data: ETLRangeRequest, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    etl = AuditsaRadioEtlService()
    s3 = S3Service()
    sns = SNSService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        fechas = etl._generate_date_range(data.fecha_inicio, data.fecha_fin)
        s3_deleted = 0
        for f in fechas:
            s3_prefix = f"rawdata/radio/{f}/"
            s3_uri = f"s3://{s3.bucket}/{s3_prefix}"
            log_s3 = log.start(source=s3_uri, instruction="DELETE", total_rows=0)
            count = s3.delete_prefix(s3_prefix)
            log.complete(log_s3, total_records=count)
            s3_deleted += count

        log_db = log.start(source="public.auditsa_api_radio", instruction="DELETE", total_rows=0)
        db_result = etl.delete_radio_data_range(data.fecha_inicio, data.fecha_fin, db)
        log.complete(log_db, total_records=db_result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="radio",
            aggregate_id=f"{data.fecha_inicio}_{data.fecha_fin}",
            payload={"fecha_inicio": data.fecha_inicio, "fecha_fin": data.fecha_fin,
                     "s3_objects_deleted": s3_deleted, "db_records_deleted": db_result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete-range")
        sns.publish(SNS_TOPIC, "DataDeleted", {"tipo": "radio", "fecha_inicio": data.fecha_inicio,
                                                "fecha_fin": data.fecha_fin, "event_id": event_id})

        return {**db_result, "s3_objects_deleted": s3_deleted, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fix-nulls", summary="Normalizar valores inválidos en todas las columnas de auditsa_api_radio")
async def fix_nulls(db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    """
    Recorre auditsa_api_radio y reemplaza por 'N.A.' todos los registros donde
    columnas de texto sean NULL, vacías, 'nan', 'None', 'N/A' o 'null'.
    Columnas numéricas con NULL se reemplazan por 0.
    """
    try:
        etl = AuditsaRadioEtlService()
        return etl.fix_nulls_in_db(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{fecha}", summary="Eliminar datos de Radio para una fecha")
async def delete_data(fecha: str, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    etl = AuditsaRadioEtlService()
    s3 = S3Service()
    sns = SNSService()
    es = EventStoreService(db)
    log = LogService(db)
    try:
        s3_prefix = f"rawdata/radio/{fecha}/"
        s3_uri = f"s3://{s3.bucket}/{s3_prefix}"
        log_s3 = log.start(source=s3_uri, instruction="DELETE", total_rows=0)
        s3_deleted = s3.delete_prefix(s3_prefix)
        log.complete(log_s3, total_records=s3_deleted)

        log_db = log.start(source="public.auditsa_api_radio", instruction="DELETE", total_rows=0)
        db_result = etl.delete_radio_data(fecha, db)
        log.complete(log_db, total_records=db_result["records_deleted"])

        event_id = es.publish(
            event_type="DataDeleted",
            aggregate_type="radio",
            aggregate_id=fecha,
            payload={"fecha": fecha, "s3_objects_deleted": s3_deleted, "db_records_deleted": db_result["records_deleted"]},
        )
        es.mark_processed(event_id, consumer_id="api-delete")
        sns.publish(SNS_TOPIC, "DataDeleted", {"tipo": "radio", "fecha": fecha, "event_id": event_id})

        return {**db_result, "s3_objects_deleted": s3_deleted, "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
