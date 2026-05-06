"""
DLQ Service — Dead Letter Queue management para EDA_Data_Ingest.
Integra la gestión de mensajes fallidos con el Event Store (domain_events).

Flujo de fallo:
  SNS → SQS spark-queue → Spark falla 3 veces → SQS spark-dlq
  Al hacer replay: spark-dlq → spark-queue → Spark reintenta
                              + domain_event status → PENDING (para ser reintentado)
"""

import boto3
import json
import logging
import os
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from models.domain_events import DomainEvent

logger = logging.getLogger(__name__)

# Mapeo: tipo → (cola principal Spark, DLQ)
QUEUE_MAP = {
    "tv":       ("tv-spark-queue",       "tv-spark-dlq"),
    "radio":    ("radio-spark-queue",    "radio-spark-dlq"),
    "impresos": ("impresos-spark-queue", "impresos-spark-dlq"),
}


class DLQService:
    """Gestión de Dead Letter Queues con integración al Event Store."""

    def __init__(self):
        self.client = boto3.client(
            "sqs",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        self.logger = logger
        self._url_cache: Dict[str, str] = {}

    def _queue_url(self, queue_name: str) -> str:
        if queue_name not in self._url_cache:
            response = self.client.get_queue_url(QueueName=queue_name)
            self._url_cache[queue_name] = response["QueueUrl"]
        return self._url_cache[queue_name]

    def get_stats(self, db: Optional[Session] = None) -> Dict:
        """
        Retorna conteo de mensajes fallidos en cada DLQ.
        Si se provee db, también incluye conteo de domain_events FAILED.
        """
        stats = {}
        for tipo, (main_q, dlq_name) in QUEUE_MAP.items():
            try:
                url = self._queue_url(dlq_name)
                attrs = self.client.get_queue_attributes(
                    QueueUrl=url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                    ],
                )["Attributes"]
                entry = {
                    "dlq": dlq_name,
                    "messages_available": int(attrs.get("ApproximateNumberOfMessages", 0)),
                    "messages_in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
                }
                if db is not None:
                    failed_events = (
                        db.query(DomainEvent)
                        .filter_by(aggregate_type=tipo, status="FAILED")
                        .count()
                    )
                    entry["failed_domain_events"] = failed_events
                stats[tipo] = entry
            except Exception as e:
                self.logger.error(f"Error obteniendo stats DLQ {tipo}: {e}")
                stats[tipo] = {"dlq": dlq_name, "error": str(e)}
        return stats

    def list_messages(self, tipo: str, max_messages: int = 10) -> List[Dict]:
        """
        Lee mensajes del DLQ sin consumirlos definitivamente.
        VisibilityTimeout=0 hace que los mensajes reaparezcan de inmediato.
        """
        if tipo not in QUEUE_MAP:
            raise ValueError(f"Tipo inválido: {tipo}. Válidos: {list(QUEUE_MAP)}")
        _, dlq_name = QUEUE_MAP[tipo]
        messages = []
        try:
            response = self.client.receive_message(
                QueueUrl=self._queue_url(dlq_name),
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=1,
                VisibilityTimeout=0,
                AttributeNames=["All"],
            )
            for msg in response.get("Messages", []):
                body = msg.get("Body", "")
                try:
                    body_parsed = json.loads(body)
                    # El cuerpo puede ser un envelope SNS — extraer el mensaje interno
                    if isinstance(body_parsed, dict) and body_parsed.get("Type") == "Notification":
                        inner = json.loads(body_parsed.get("Message", "{}"))
                        body_parsed = {"sns_envelope": True, "inner": inner, "raw": body_parsed}
                except Exception:
                    body_parsed = body
                messages.append({
                    "message_id": msg["MessageId"],
                    "receipt_handle": msg["ReceiptHandle"],
                    "body": body_parsed,
                    "receive_count": msg.get("Attributes", {}).get("ApproximateReceiveCount", "?"),
                    "sent_at": msg.get("Attributes", {}).get("SentTimestamp", "?"),
                })
        except Exception as e:
            self.logger.error(f"Error listando DLQ {tipo}: {e}")
        return messages

    def replay_all(self, tipo: str, db: Optional[Session] = None) -> Dict:
        """
        Mueve todos los mensajes del DLQ de vuelta a la cola Spark.
        Opcionalmente resetea los domain_events FAILED del tipo a PENDING.
        """
        if tipo not in QUEUE_MAP:
            raise ValueError(f"Tipo inválido: {tipo}. Válidos: {list(QUEUE_MAP)}")
        main_q, dlq_name = QUEUE_MAP[tipo]
        replayed = 0
        errors = 0
        events_reset = 0

        while True:
            try:
                response = self.client.receive_message(
                    QueueUrl=self._queue_url(dlq_name),
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1,
                    VisibilityTimeout=30,
                )
                msgs = response.get("Messages", [])
                if not msgs:
                    break
                for msg in msgs:
                    try:
                        self.client.send_message(
                            QueueUrl=self._queue_url(main_q),
                            MessageBody=msg["Body"],
                        )
                        self.client.delete_message(
                            QueueUrl=self._queue_url(dlq_name),
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                        replayed += 1
                        self.logger.info(f"[DLQ] Replay {tipo}: {msg['MessageId']} → {main_q}")
                    except Exception as e:
                        self.logger.error(f"[DLQ] Error replay {msg['MessageId']}: {e}")
                        errors += 1
            except Exception as e:
                self.logger.error(f"[DLQ] Error en replay_all {tipo}: {e}")
                break

        # Resetear domain_events FAILED → PENDING para que el Event Store refleje el replay
        if db is not None and replayed > 0:
            try:
                failed = (
                    db.query(DomainEvent)
                    .filter_by(aggregate_type=tipo, status="FAILED")
                    .all()
                )
                for event in failed:
                    event.status = "PENDING"
                    event.processed_at = None
                    event.consumer_id = "dlq-replay"
                db.commit()
                events_reset = len(failed)
                self.logger.info(f"[DLQ] {events_reset} domain_events FAILED → PENDING para {tipo}")
            except Exception as e:
                self.logger.error(f"[DLQ] Error reseteando domain_events: {e}")

        return {
            "tipo": tipo,
            "main_queue": main_q,
            "replayed": replayed,
            "errors": errors,
            "domain_events_reset": events_reset,
        }

    def purge(self, tipo: str, db: Optional[Session] = None) -> Dict:
        """
        Elimina permanentemente todos los mensajes del DLQ.
        Opcionalmente marca los domain_events FAILED como FAILED definitivo (sin cambio).
        """
        if tipo not in QUEUE_MAP:
            raise ValueError(f"Tipo inválido: {tipo}. Válidos: {list(QUEUE_MAP)}")
        _, dlq_name = QUEUE_MAP[tipo]
        try:
            self.client.purge_queue(QueueUrl=self._queue_url(dlq_name))
            self.logger.info(f"[DLQ] Purgado: {dlq_name}")
            return {"tipo": tipo, "dlq": dlq_name, "status": "purged"}
        except Exception as e:
            self.logger.error(f"[DLQ] Error purgando {dlq_name}: {e}")
            raise
