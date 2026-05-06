"""
Servicio de S3 (LocalStack / AWS)
Maneja la subida y descarga de datos crudos al Data Lake
"""

import boto3
import json
import logging
import os
from typing import List, Dict

logger = logging.getLogger(__name__)


class S3Service:
    """Servicio para interactuar con S3 (LocalStack en desarrollo, AWS en producción)"""

    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        self.bucket = os.getenv("S3_BUCKET", "raw-data")
        self.logger = logger

    def upload_json(self, key: str, data: List[Dict]) -> str:
        """
        Sube datos crudos como JSON a S3.

        Args:
            key: Ruta del objeto en S3 (ej: raw/tv/20251116/data.json)
            data: Lista de diccionarios a serializar

        Returns:
            S3 URI del objeto subido
        """
        try:
            body = json.dumps(data, default=str, ensure_ascii=False)
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            s3_uri = f"s3://{self.bucket}/{key}"
            self.logger.info(f"Datos subidos a S3: {s3_uri} ({len(data)} registros)")
            return s3_uri
        except Exception as e:
            self.logger.error(f"Error subiendo datos a S3 ({key}): {e}")
            raise

    def download_json(self, key: str) -> List[Dict]:
        """
        Descarga y deserializa un JSON desde S3.

        Args:
            key: Ruta del objeto en S3

        Returns:
            Lista de diccionarios
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            self.logger.info(f"Descargados {len(data)} registros desde s3://{self.bucket}/{key}")
            return data
        except Exception as e:
            self.logger.error(f"Error descargando desde S3 ({key}): {e}")
            raise

    def delete_prefix(self, prefix: str) -> int:
        """
        Elimina todos los objetos en S3 bajo un prefijo dado.

        Args:
            prefix: Prefijo del path en S3 (ej: rawdata/tv/20260101/)

        Returns:
            Número de objetos eliminados
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            deleted = 0
            for page in pages:
                objects = page.get("Contents", [])
                if objects:
                    self.client.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
                    )
                    deleted += len(objects)
            self.logger.info(f"S3 DELETE: {deleted} objeto(s) eliminado(s) bajo s3://{self.bucket}/{prefix}")
            return deleted
        except Exception as e:
            self.logger.error(f"Error eliminando objetos S3 bajo {prefix}: {e}")
            raise

    def check_connection(self) -> bool:
        """Verifica que la conexión con S3 funciona."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except Exception:
            return False
