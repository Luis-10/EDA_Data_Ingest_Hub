"""
Event Store — tabla domain_events
Registro inmutable de todos los eventos de dominio del sistema.
Cada evento representa un hecho de negocio ocurrido: DataIngested, DataProcessed, DataDeleted.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text
from config.database import Base


class DomainEvent(Base):
    __tablename__ = "domain_events"
    __table_args__ = {"schema": "public"}

    id             = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    event_id       = Column(String(36), unique=True, nullable=False)   # UUID — identificador inmutable
    event_type     = Column(String(100), nullable=False)               # DataIngested | DataProcessed | DataDeleted
    aggregate_type = Column(String(50), nullable=False)                # tv | radio | impresos
    aggregate_id   = Column(String(50), nullable=False)                # fecha YYYYMMDD (ID del agregado)
    payload        = Column(Text, nullable=False)                      # JSON con datos del evento
    occurred_at    = Column(DateTime, nullable=False)                  # Cuándo ocurrió (inmutable)
    processed_at   = Column(DateTime, nullable=True)                   # Cuándo fue procesado por un consumer
    status         = Column(String(20), nullable=False, default="PENDING")  # PENDING | PROCESSED | FAILED
    consumer_id    = Column(String(100), nullable=True)                # ID del consumer que procesó (ej: spark-processor-1)
