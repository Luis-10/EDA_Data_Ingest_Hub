"""
Router de Events — API del Event Store
Permite consultar el estado de los eventos de dominio y hacer replay manual.
"""

import json
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from config.database import get_db
from config.auth import require_admin, require_reader
from services.event_store_service import EventStoreService
from services.sns_service import SNSService

router = APIRouter(prefix="/api/events", tags=["Events"])
logger = logging.getLogger(__name__)


def _serialize_event(event) -> dict:
    return {
        "id": event.id,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
        "payload": json.loads(event.payload),
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
        "processed_at": event.processed_at.isoformat() if event.processed_at else None,
        "status": event.status,
        "consumer_id": event.consumer_id,
    }


@router.get("/", summary="Consultar eventos del Event Store")
async def get_events(
    tipo: Optional[str] = Query(default=None, description="Tipo de medio: tv | radio | impresos"),
    fecha: Optional[str] = Query(default=None, description="Fecha YYYYMMDD"),
    event_type: Optional[str] = Query(default=None, description="Tipo de evento: DataIngested | DataProcessed | DataDeleted"),
    status: Optional[str] = Query(default=None, description="Estado: PENDING | PROCESSED | FAILED"),
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    es = EventStoreService(db)
    events = es.get_events(
        aggregate_type=tipo,
        aggregate_id=fecha,
        event_type=event_type,
        status=status,
        limit=limit,
    )
    return {
        "total": len(events),
        "filters": {"tipo": tipo, "fecha": fecha, "event_type": event_type, "status": status},
        "events": [_serialize_event(e) for e in events],
    }


@router.get("/pending", summary="Eventos pendientes de procesamiento")
async def get_pending(
    tipo: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    es = EventStoreService(db)
    events = es.get_events(aggregate_type=tipo, status="PENDING", limit=200)
    return {
        "total": len(events),
        "status": "PENDING",
        "events": [_serialize_event(e) for e in events],
    }


@router.get("/{event_id}", summary="Detalle de un evento")
async def get_event(event_id: str, db: Session = Depends(get_db), _: dict = Depends(require_reader)):
    es = EventStoreService(db)
    event = es.get_by_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Evento {event_id} no encontrado")
    return _serialize_event(event)


@router.post("/{event_id}/replay", summary="Reencolar un evento para reprocesamiento")
async def replay_event(event_id: str, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    """
    Reencola un evento DataIngested al topic SNS correspondiente.
    Útil para reprocesar eventos que fallaron o quedaron en PENDING.
    """
    es = EventStoreService(db)
    event = es.get_by_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Evento {event_id} no encontrado")
    if event.event_type != "DataIngested":
        raise HTTPException(status_code=400, detail="Solo se pueden reencolar eventos DataIngested")

    payload = json.loads(event.payload)
    sns = SNSService()
    topic_name = sns.topic_name_for(event.aggregate_type)

    # Publicar de nuevo al topic SNS para que el consumer lo procese
    sns.publish(
        topic_name=topic_name,
        event_type="DataIngested",
        payload={**payload, "replay": True, "original_event_id": event_id},
    )

    # Resetear estado a PENDING para que el consumer actualice
    event.status = "PENDING"
    event.processed_at = None
    event.consumer_id = None
    db.commit()

    logger.info(f"Replay solicitado: event_id={event_id} | tipo={event.aggregate_type} | fecha={event.aggregate_id}")
    return {
        "status": "replayed",
        "event_id": event_id,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
        "topic": topic_name,
        "message": "Evento reencolado al topic SNS. El Spark Consumer lo procesará nuevamente.",
    }
