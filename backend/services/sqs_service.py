"""
Servicio de SQS (LocalStack / AWS)
Maneja el envío y recepción de mensajes de ingesta
"""

import boto3
import json
import logging
import os
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class SQSService:
    """Servicio para interactuar con SQS (LocalStack en desarrollo, AWS en producción)"""

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
        """Obtiene la URL real de la cola desde LocalStack/AWS y la cachea."""
        if queue_name not in self._url_cache:
            response = self.client.get_queue_url(QueueName=queue_name)
            self._url_cache[queue_name] = response["QueueUrl"]
            self.logger.info(f"URL de cola '{queue_name}': {self._url_cache[queue_name]}")
        return self._url_cache[queue_name]

    def send_message(self, queue_name: str, message: Dict) -> Optional[str]:
        """
        Envía un mensaje a una cola SQS.

        Args:
            queue_name: Nombre de la cola (ej: tv-ingestion-queue)
            message: Diccionario con el cuerpo del mensaje

        Returns:
            MessageId del mensaje enviado, o None si falla
        """
        try:
            response = self.client.send_message(
                QueueUrl=self._queue_url(queue_name),
                MessageBody=json.dumps(message, default=str),
            )
            msg_id = response["MessageId"]
            self.logger.info(f"Mensaje enviado a {queue_name}: {msg_id}")
            return msg_id
        except Exception as e:
            self.logger.error(f"Error enviando mensaje a {queue_name}: {e}")
            return None

    def receive_messages(self, queue_name: str, max_messages: int = 10) -> List[Dict]:
        """
        Recibe mensajes de una cola SQS (long polling 5s).

        Args:
            queue_name: Nombre de la cola
            max_messages: Máximo de mensajes a recibir (1-10)

        Returns:
            Lista de mensajes SQS
        """
        try:
            response = self.client.receive_message(
                QueueUrl=self._queue_url(queue_name),
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=5,
            )
            return response.get("Messages", [])
        except Exception as e:
            self.logger.error(f"Error recibiendo mensajes de {queue_name}: {e}")
            return []

    def delete_message(self, queue_name: str, receipt_handle: str) -> None:
        """
        Elimina un mensaje de la cola tras procesarlo.

        Args:
            queue_name: Nombre de la cola
            receipt_handle: Handle del mensaje a eliminar
        """
        try:
            self.client.delete_message(
                QueueUrl=self._queue_url(queue_name),
                ReceiptHandle=receipt_handle,
            )
        except Exception as e:
            self.logger.error(f"Error eliminando mensaje de {queue_name}: {e}")
