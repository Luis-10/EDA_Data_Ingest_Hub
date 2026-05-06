"""
Servicio de logging para operaciones ETL.
Registra cargas a S3 y a PostgreSQL en la tabla auditsa_api_log.
Dual-write: replica cada operación en SQL Server appSAC.app.auditsa_api_log.
"""

import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from config.database import SqlServerSacSessionLocal
from models.api_auditsa_logs import db_auditsa_api_log
from models.sqlserver_users import ss_app_auditsa_api_log

logger = logging.getLogger(__name__)


class LogService:
    """Gestiona el ciclo de vida de un registro de log ETL."""

    def __init__(self, db: Session):
        self.db = db

    def start(self, source: str, instruction: str, total_rows: int = 0) -> int:
        """
        Crea un registro en estado RECORDING.

        Args:
            source: Ruta S3 (s3://...) o nombre de tabla (public.auditsa_api_tv)
            instruction: Instrucción SQL/operación (INSERT | DELETE)
            total_rows: Total de filas disponibles en la fuente (API)

        Returns:
            id del registro creado
        """
        log = db_auditsa_api_log(
            source=source,
            date_time=datetime.now(timezone.utc).replace(tzinfo=None),
            instruction=instruction,
            total_rows=total_rows,
            total_records=0,
            status="RECORDING",
        )
        self.db.add(log)
        self.db.commit()
        self.db.refresh(log)

        ss_session = SqlServerSacSessionLocal()
        try:
            ss_session.add(ss_app_auditsa_api_log(
                id=log.id,
                source=source,
                date_time=log.date_time,
                instruction=instruction,
                total_rows=total_rows,
                total_records=0,
                status="RECORDING",
            ))
            ss_session.commit()
        except Exception as e:
            ss_session.rollback()
            logger.warning("SQL Server appSAC log.start failed (non-fatal): %s", e)
        finally:
            ss_session.close()

        return log.id

    def complete(self, log_id: int, total_records: int, status: str = "SUCCESS") -> None:
        """
        Actualiza el registro con el resultado final.

        Args:
            log_id: id del registro a actualizar
            total_records: Filas efectivamente cargadas
            status: Estado final (SUCCESS u otro en caso de error)
        """
        log = self.db.query(db_auditsa_api_log).filter_by(id=log_id).first()
        if log:
            log.total_records = total_records
            log.status = status
            self.db.commit()

        ss_session = SqlServerSacSessionLocal()
        try:
            ss_log = ss_session.query(ss_app_auditsa_api_log).filter_by(id=log_id).first()
            if ss_log:
                ss_log.total_records = total_records
                ss_log.status = status
                ss_session.commit()
        except Exception as e:
            ss_session.rollback()
            logger.warning("SQL Server appSAC log.complete failed (non-fatal): %s", e)
        finally:
            ss_session.close()
