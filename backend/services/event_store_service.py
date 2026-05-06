"""
Event Store Service
Gestiona la persistencia y consulta de eventos de dominio en la tabla domain_events.
Cada evento es un registro inmutable que representa un hecho de negocio.
Dual-write: replica cada operación en SQL Server appSAC.app.domain_events.
"""

import uuid
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy.orm import Session

from config.database import SqlServerSacSessionLocal
from models.domain_events import DomainEvent
from models.sqlserver_users import ss_app_domain_events

logger = logging.getLogger(__name__)


class EventStoreService:
    """CRUD del Event Store sobre la tabla domain_events."""

    def __init__(self, db: Session):
        self.db = db

    def publish(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict,
    ) -> str:
        """
        Registra un nuevo evento de dominio como PENDING.
        Idempotente: si ya existe un evento PENDING con el mismo
        (event_type, aggregate_type, aggregate_id), retorna el event_id existente
        en vez de crear un duplicado.

        Args:
            event_type: Tipo de evento (DataIngested | DataProcessed | DataDeleted)
            aggregate_type: Tipo de agregado (tv | radio | impresos)
            aggregate_id: ID del agregado (fecha YYYYMMDD)
            payload: Datos del evento (s3_key, records, s3_uri, etc.)

        Returns:
            event_id (UUID) del evento creado o existente
        """
        existing = (
            self.db.query(DomainEvent)
            .filter_by(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                status="PENDING",
            )
            .first()
        )
        if existing:
            logger.info(
                "EventStore: IDEMPOTENT — evento PENDING ya existe | %s | %s/%s | id=%s",
                event_type, aggregate_type, aggregate_id, existing.event_id,
            )
            existing.payload = json.dumps(payload, default=str)
            self.db.commit()

            # Sincronizar actualización de payload en SQL Server
            ss_session = SqlServerSacSessionLocal()
            try:
                ss_ev = ss_session.query(ss_app_domain_events).filter_by(event_id=existing.event_id).first()
                if ss_ev:
                    ss_ev.payload = existing.payload
                    ss_session.commit()
            except Exception as e:
                ss_session.rollback()
                logger.warning("SQL Server appSAC event publish (idempotent) failed (non-fatal): %s", e)
            finally:
                ss_session.close()

            return existing.event_id

        event_id = str(uuid.uuid4())
        event = DomainEvent(
            event_id=event_id,
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=json.dumps(payload, default=str),
            occurred_at=datetime.now(timezone.utc).replace(tzinfo=None),
            status="PENDING",
        )
        self.db.add(event)
        self.db.commit()
        self.db.refresh(event)
        logger.info("EventStore: %s | %s/%s | id=%s", event_type, aggregate_type, aggregate_id, event_id)

        # Dual-write a SQL Server appSAC.app.domain_events
        ss_session = SqlServerSacSessionLocal()
        try:
            ss_session.add(ss_app_domain_events(
                id=event.id,
                event_id=event_id,
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                payload=event.payload,
                occurred_at=event.occurred_at,
                status="PENDING",
            ))
            ss_session.commit()
        except Exception as e:
            ss_session.rollback()
            logger.warning("SQL Server appSAC event publish failed (non-fatal): %s", e)
        finally:
            ss_session.close()

        return event_id

    def mark_processed(self, event_id: str, consumer_id: str) -> None:
        """Marca un evento como PROCESSED."""
        event = self.db.query(DomainEvent).filter_by(event_id=event_id).first()
        if event:
            event.status = "PROCESSED"
            event.processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            event.consumer_id = consumer_id
            self.db.commit()

        ss_session = SqlServerSacSessionLocal()
        try:
            ss_ev = ss_session.query(ss_app_domain_events).filter_by(event_id=event_id).first()
            if ss_ev:
                ss_ev.status = "PROCESSED"
                ss_ev.processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                ss_ev.consumer_id = consumer_id
                ss_session.commit()
        except Exception as e:
            ss_session.rollback()
            logger.warning("SQL Server appSAC event mark_processed failed (non-fatal): %s", e)
        finally:
            ss_session.close()

    def mark_failed(self, event_id: str, consumer_id: str) -> None:
        """Marca un evento como FAILED."""
        event = self.db.query(DomainEvent).filter_by(event_id=event_id).first()
        if event:
            event.status = "FAILED"
            event.processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            event.consumer_id = consumer_id
            self.db.commit()

        ss_session = SqlServerSacSessionLocal()
        try:
            ss_ev = ss_session.query(ss_app_domain_events).filter_by(event_id=event_id).first()
            if ss_ev:
                ss_ev.status = "FAILED"
                ss_ev.processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                ss_ev.consumer_id = consumer_id
                ss_session.commit()
        except Exception as e:
            ss_session.rollback()
            logger.warning("SQL Server appSAC event mark_failed failed (non-fatal): %s", e)
        finally:
            ss_session.close()

    def get_by_id(self, event_id: str) -> Optional[DomainEvent]:
        """Retorna un evento por su event_id."""
        return self.db.query(DomainEvent).filter_by(event_id=event_id).first()

    def get_events(
        self,
        aggregate_type: Optional[str] = None,
        aggregate_id: Optional[str] = None,
        event_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[DomainEvent]:
        """Consulta eventos con filtros opcionales."""
        q = self.db.query(DomainEvent)
        if aggregate_type:
            q = q.filter(DomainEvent.aggregate_type == aggregate_type)
        if aggregate_id:
            q = q.filter(DomainEvent.aggregate_id == aggregate_id)
        if event_type:
            q = q.filter(DomainEvent.event_type == event_type)
        if status:
            q = q.filter(DomainEvent.status == status)
        return q.order_by(DomainEvent.occurred_at.desc()).limit(limit).all()
