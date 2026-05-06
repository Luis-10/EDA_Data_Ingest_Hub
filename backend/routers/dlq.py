"""
Router DLQ — Dead Letter Queue management endpoints para EDA_Data_Ingest.
Integrado con el Event Store: replay también resetea domain_events FAILED → PENDING.

Endpoints:
  GET    /api/dlq              → stats de los 3 DLQs + conteo domain_events FAILED
  GET    /api/dlq/{tipo}       → mensajes en el DLQ del tipo (tv/radio/impresos)
  POST   /api/dlq/{tipo}/replay → mueve mensajes al spark-queue + resetea domain_events
  DELETE /api/dlq/{tipo}       → purga el DLQ
"""

import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from config.database import get_db
from config.auth import require_admin, require_reader
from services.dlq_service import DLQService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dlq", tags=["DLQ"])

TIPOS_VALIDOS = ["tv", "radio", "impresos"]


def _validar_tipo(tipo: str):
    if tipo not in TIPOS_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Tipo inválido: '{tipo}'. Válidos: {TIPOS_VALIDOS}")


@router.get("", summary="Stats de todos los DLQs + domain_events FAILED")
async def get_dlq_stats(db: Session = Depends(get_db), _: dict = Depends(require_reader)):
    """
    Retorna el conteo de mensajes fallidos en cada DLQ
    y el número de domain_events con status=FAILED en el Event Store.
    """
    try:
        dlq = DLQService()
        stats = dlq.get_stats(db=db)
        total_msgs = sum(v.get("messages_available", 0) for v in stats.values() if "error" not in v)
        total_events = sum(v.get("failed_domain_events", 0) for v in stats.values() if "error" not in v)
        return {
            "status": "ok",
            "total_failed_messages": total_msgs,
            "total_failed_domain_events": total_events,
            "queues": stats,
        }
    except Exception as e:
        logger.error(f"Error obteniendo stats DLQ: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{tipo}", summary="Listar mensajes fallidos de un tipo")
async def list_dlq_messages(tipo: str, max_messages: int = 10, _: dict = Depends(require_reader)):
    """
    Lista hasta `max_messages` mensajes en el DLQ del tipo indicado.
    Los mensajes NO se eliminan — se pueden inspeccionar y luego hacer replay.
    Detecta automáticamente el envelope SNS si aplica.
    """
    _validar_tipo(tipo)
    try:
        dlq = DLQService()
        messages = dlq.list_messages(tipo, max_messages)
        return {
            "tipo": tipo,
            "count": len(messages),
            "messages": messages,
        }
    except Exception as e:
        logger.error(f"Error listando DLQ {tipo}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{tipo}/replay", summary="Replay: mover mensajes del DLQ al spark-queue + resetear domain_events")
async def replay_dlq(tipo: str, db: Session = Depends(get_db), _: dict = Depends(require_admin)):
    """
    Mueve todos los mensajes del DLQ de vuelta a la cola Spark principal.
    Además resetea los domain_events FAILED del tipo a PENDING en el Event Store,
    para que el flujo EDA quede consistente con el reintento.
    """
    _validar_tipo(tipo)
    try:
        dlq = DLQService()
        result = dlq.replay_all(tipo, db=db)
        return {
            "status": "ok",
            "message": (
                f"{result['replayed']} mensajes reencolados en '{result['main_queue']}'. "
                f"{result['domain_events_reset']} domain_events reseteados a PENDING."
            ),
            **result,
        }
    except Exception as e:
        logger.error(f"Error en replay DLQ {tipo}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{tipo}", summary="Purgar DLQ (eliminar todos los mensajes fallidos)")
async def purge_dlq(tipo: str, _: dict = Depends(require_admin)):
    """
    Elimina permanentemente todos los mensajes del DLQ.
    Los domain_events con status=FAILED permanecen en el Event Store como registro histórico.
    """
    _validar_tipo(tipo)
    try:
        dlq = DLQService()
        result = dlq.purge(tipo)
        return {"status": "ok", "message": f"DLQ '{result['dlq']}' purgado.", **result}
    except Exception as e:
        logger.error(f"Error purgando DLQ {tipo}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
