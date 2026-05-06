"""
Servicio SNS — Event Bus central
Publica eventos de dominio al topic SNS correspondiente.
Usado para eventos que no pasan por S3 (DELETE, eventos de control).
"""

import boto3
import json
import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)


class SNSService:
    """Publica eventos al Event Bus central (SNS)."""

    def __init__(self):
        self.client = boto3.client(
            "sns",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        self.base_arn = os.getenv("SNS_BASE_ARN", "arn:aws:sns:us-east-1:000000000000")
        self.logger = logger

    def _topic_arn(self, topic_name: str) -> str:
        return f"{self.base_arn}:{topic_name}"

    def publish(self, topic_name: str, event_type: str, payload: Dict) -> str:
        """
        Publica un evento al topic SNS indicado.

        Args:
            topic_name: Nombre del topic (ej: tv-events)
            event_type: Tipo de evento (ej: DataDeleted)
            payload: Datos del evento

        Returns:
            MessageId del mensaje publicado
        """
        try:
            topic_arn = self._topic_arn(topic_name)
            message = json.dumps({"event_type": event_type, **payload}, default=str)
            response = self.client.publish(
                TopicArn=topic_arn,
                Message=message,
                Subject=event_type,
                MessageAttributes={
                    "event_type": {"DataType": "String", "StringValue": event_type}
                },
            )
            msg_id = response["MessageId"]
            self.logger.info(f"SNS publish → {topic_name} | {event_type} | MessageId={msg_id}")
            return msg_id
        except Exception as e:
            self.logger.error(f"Error publicando a SNS topic {topic_name}: {e}")
            raise

    def topic_name_for(self, tipo: str) -> str:
        """Retorna el nombre del topic SNS para un tipo de medio."""
        return f"{tipo}-events"
