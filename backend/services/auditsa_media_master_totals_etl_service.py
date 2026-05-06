"""
Servicio ETL especializado para datos de GetMediaMasterTotals de Auditsa
Maneja Extract, Transform, Load para los totales agregados del maestro de medios
"""

import logging
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import text, func

from config.database import SessionLocal
from models.api_auditsa_media_master_totals import db_auditsa_media_master_totals
from services.auditsa_api_services import AuditsaApiService
from services.sqlserver_write_service import write_to_sqlserver


class AuditsaMediaMasterTotalsEtlService:
    """Servicio ETL para totales agregados del maestro de medios de Auditsa"""

    def __init__(self):
        self.logger = logging.getLogger(
            "services.auditsa_media_master_totals_etl_service"
        )
        self.api_service = AuditsaApiService()

    def _validate_fecha_format(self, fecha: str) -> None:
        """Valida el formato de fecha YYYYMMDD"""
        if len(fecha) != 8 or not fecha.isdigit():
            raise ValueError("Fecha debe estar en formato YYYYMMDD (ej: 20251001)")
        try:
            datetime.strptime(fecha, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Fecha inválida: {fecha}")

    def _parse_fecha(self, fecha: str) -> date:
        return datetime.strptime(fecha, "%Y%m%d").date()

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------

    def extract_data(
        self,
        fecha_inicio: str,
        fecha_fin: str,
        extra_param: Optional[str] = None,
    ) -> List[Dict]:
        """
        Extract: Consulta GetMediaMasterTotals para el rango dado.

        Args:
            fecha_inicio: Fecha de inicio YYYYMMDD
            fecha_fin: Fecha de fin YYYYMMDD
            extra_param: Nombre del parámetro booleario adicional opcional (ej. "byMedio")

        Returns: Lista de diccionarios con totales crudos
        """
        try:
            self._validate_fecha_format(fecha_inicio)
            self._validate_fecha_format(fecha_fin)

            self.logger.info(
                f"📥 EXTRACT: Consultando MediaMasterTotals {fecha_inicio} - {fecha_fin}"
            )

            raw_data = self.api_service.get_media_master_totals_data(
                fecha_inicio, fecha_fin, extra_param
            )

            if not raw_data:
                self.logger.warning("📥 EXTRACT: Sin datos en el rango solicitado")
                return []

            self.logger.info(f"📥 EXTRACT: {len(raw_data)} registros obtenidos")
            return raw_data

        except Exception as e:
            self.logger.error(f"❌ EXTRACT ERROR: {str(e)}")
            raise

    # -------------------------------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------------------------------

    def transform_data(
        self, raw_data: List[Dict], fecha_inicio: str, fecha_fin: str
    ) -> List[Dict]:
        """
        Transform: Limpia, tipifica y enriquece los registros con las fechas del rango.

        Args:
            raw_data: Datos crudos de la API
            fecha_inicio: Fecha de inicio para almacenar en cada registro
            fecha_fin: Fecha de fin para almacenar en cada registro

        Returns: Lista de diccionarios transformados y validados
        """
        try:
            if not raw_data:
                self.logger.warning("🔄 TRANSFORM: No hay datos para transformar")
                return []

            self.logger.info(
                f"🔄 TRANSFORM: Transformando {len(raw_data)} registros"
            )

            df = pd.DataFrame(raw_data)

            df = self._apply_data_transformations(df, fecha_inicio, fecha_fin)
            df = self._validate_data_quality(df)

            transformed_data = df.to_dict("records")

            self.logger.info(
                f"🔄 TRANSFORM: {len(transformed_data)} registros válidos de {len(raw_data)} originales"
            )
            return transformed_data

        except Exception as e:
            self.logger.error(f"❌ TRANSFORM ERROR: {str(e)}")
            self.logger.warning("🔄 TRANSFORM: Retornando lista vacía por error")
            return []

    def _apply_data_transformations(
        self, df: pd.DataFrame, fecha_inicio: str, fecha_fin: str
    ) -> pd.DataFrame:
        """Aplica transformaciones y limpieza a los datos de totales"""

        from services.string_normalizer import normalize_dataframe_columns

        # Inyectar fechas del rango de consulta en cada registro
        df["FechaInicio"] = self._parse_fecha(fecha_inicio)
        df["FechaFin"] = self._parse_fecha(fecha_fin)

        # Columnas string a normalizar (mayúsculas, sin acentos)
        string_columns = [
            "Medio", "Canal", "GEstacion", "FHoraria", "TipoSpot",
            "Genero", "TipoMaster", "Anunciante", "Marca", "Submarca",
            "Producto", "Segmento", "Industria", "Mercado",
        ]

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        normalize_dataframe_columns(df, string_columns)

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
                df[col] = df[col].str.slice(0, 255)

        # Periodo: preservar tal cual (es string de display)
        if "Periodo" in df.columns:
            df["Periodo"] = df["Periodo"].astype(str).str.strip()
            df["Periodo"] = (
                df["Periodo"].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
            )

        # Version / Testigo: texto largo
        for col in ("Version", "Testigo"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = (
                    df[col].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
                )
            else:
                df[col] = "N.A."

        # Campos numéricos enteros — NoCorte y DTeorica tienen muchos "" → None
        nullable_int_cols = ["NoCorte", "DTeorica"]
        for col in nullable_int_cols:
            if col in df.columns:
                df[col] = df[col].replace("", None)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Mantener como nullable Int64 (None donde estaba "")
                df[col] = df[col].astype("Int64")

        # IdMaster siempre presente
        if "IdMaster" in df.columns:
            df["IdMaster"] = pd.to_numeric(df["IdMaster"], errors="coerce").astype(
                "Int64"
            )

        # TotalHit: siempre presente, entero
        if "TotalHit" in df.columns:
            df["TotalHit"] = (
                pd.to_numeric(df["TotalHit"], errors="coerce").fillna(0).astype(int)
            )

        # TotalInversion: 442 registros vacíos → None/0.0
        if "TotalInversion" in df.columns:
            df["TotalInversion"] = df["TotalInversion"].replace("", None)
            df["TotalInversion"] = pd.to_numeric(
                df["TotalInversion"], errors="coerce"
            )

        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros sin IdMaster válido"""
        original_count = len(df)

        if "IdMaster" in df.columns:
            df = df[df["IdMaster"].notna()]

        excluded = original_count - len(df)
        if excluded > 0:
            self.logger.warning(
                f"🔄 VALIDATION: Excluidos {excluded} registros sin IdMaster"
            )

        self.logger.info(
            f"🔄 VALIDATION: {len(df)} de {original_count} registros validados"
        )
        return df

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------

    def load_data(
        self, transformed_data: List[Dict], db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Load: Inserta los registros en PostgreSQL y hace dual-write a SQL Server.

        Args:
            transformed_data: Datos transformados y validados
            db: Sesión de base de datos (opcional)

        Returns: Diccionario con resultado de la operación
        """
        if not transformed_data:
            return {
                "status": "success",
                "message": "No hay datos para cargar",
                "records_inserted": 0,
            }

        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(
                f"💾 LOAD: Insertando {len(transformed_data)} registros en "
                f"auditsa_api_media_master_totals"
            )

            records_to_insert = [
                db_auditsa_media_master_totals(
                    FechaInicio=record.get("FechaInicio"),
                    FechaFin=record.get("FechaFin"),
                    Periodo=record.get("Periodo"),
                    Medio=record.get("Medio"),
                    Canal=record.get("Canal"),
                    GEstacion=record.get("GEstacion"),
                    FHoraria=record.get("FHoraria"),
                    NoCorte=record.get("NoCorte"),
                    TipoSpot=record.get("TipoSpot"),
                    Genero=record.get("Genero"),
                    TipoMaster=record.get("TipoMaster"),
                    IdMaster=record.get("IdMaster"),
                    Version=record.get("Version"),
                    Anunciante=record.get("Anunciante"),
                    Marca=record.get("Marca"),
                    Submarca=record.get("Submarca"),
                    Producto=record.get("Producto"),
                    Segmento=record.get("Segmento"),
                    Industria=record.get("Industria"),
                    Mercado=record.get("Mercado"),
                    DTeorica=record.get("DTeorica"),
                    TotalHit=record.get("TotalHit"),
                    TotalInversion=record.get("TotalInversion"),
                    Testigo=record.get("Testigo") or "N.A.",
                )
                for record in transformed_data
            ]

            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i: i + batch_size]
                db_session.add_all(batch)
                db_session.flush()
                total_inserted += len(batch)

                if total_inserted % 5000 == 0:
                    self.logger.info(
                        f"💾 LOAD: {total_inserted}/{len(records_to_insert)} procesados"
                    )

            db_session.commit()
            self._fix_nulls_in_db(db_session)

            self.logger.info(f"💾 LOAD: {total_inserted} registros cargados")

            # Dual-write a SQL Server (no-fatal)
            ss_inserted, ss_error = 0, None
            try:
                ss_inserted = write_to_sqlserver(transformed_data, "media_master_totals")
            except Exception as ss_e:
                ss_error = str(ss_e)
                self.logger.error(
                    "SQL Server dual-write MediaMasterTotals falló (no-fatal): %s", ss_e
                )

            return {
                "status": "success",
                "message": "Datos de MediaMasterTotals cargados exitosamente",
                "records_inserted": total_inserted,
                "sqlserver": {"inserted": ss_inserted, "error": ss_error},
            }

        except IntegrityError as e:
            db_session.rollback()
            self.logger.error(f"❌ LOAD ERROR - Integridad: {str(e)}")
            raise SQLAlchemyError(f"Error de integridad: {str(e)}")

        except SQLAlchemyError as e:
            db_session.rollback()
            self.logger.error(f"❌ LOAD ERROR - Base de datos: {str(e)}")
            raise

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ LOAD ERROR - General: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()

    # -------------------------------------------------------------------------
    # PIPELINE
    # -------------------------------------------------------------------------

    def _generate_date_range(self, fecha_inicio: str, fecha_fin: str) -> List[str]:
        """Genera lista de fechas YYYYMMDD entre fecha_inicio y fecha_fin (inclusive)."""
        start = self._parse_fecha(fecha_inicio)
        end = self._parse_fecha(fecha_fin)
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return dates

    def run_etl_pipeline(
        self,
        fecha_inicio: str,
        fecha_fin: str,
        extra_param: Optional[str] = None,
        db: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL completo día a día para evitar timeouts en rangos largos.
        Cada día se extrae, transforma y carga de forma independiente con FechaInicio=FechaFin=día.
        """
        start_time = datetime.now()
        dates = self._generate_date_range(fecha_inicio, fecha_fin)

        self.logger.info(
            f"🚀 ETL PIPELINE: Iniciando MediaMasterTotals {fecha_inicio} - {fecha_fin} "
            f"({len(dates)} día(s))"
        )

        total_extracted = 0
        total_transformed = 0
        total_loaded = 0
        errors: List[str] = []

        for fecha in dates:
            try:
                raw_data = self.extract_data(fecha, fecha, extra_param)
                transformed_data = self.transform_data(raw_data, fecha, fecha)
                load_result = self.load_data(transformed_data, db)

                total_extracted += len(raw_data)
                total_transformed += len(transformed_data)
                total_loaded += load_result["records_inserted"]

                self.logger.info(
                    f"✅ {fecha}: {len(raw_data)} extraídos, "
                    f"{load_result['records_inserted']} cargados"
                )
            except Exception as e:
                err_msg = f"{fecha}: {str(e)}"
                errors.append(err_msg)
                self.logger.error(f"❌ ETL {fecha} FAILED: {str(e)}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if errors and total_loaded == 0:
            return {
                "status": "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "errors": errors,
                "duration_seconds": round(duration, 2),
                "message": f"Error en ETL MediaMasterTotals {fecha_inicio} - {fecha_fin}: {errors[0]}",
            }

        self.logger.info(
            f"✅ ETL PIPELINE: {total_loaded} registros cargados en {duration:.2f}s"
            + (f" — {len(errors)} días con error" if errors else "")
        )

        return {
            "status": "success" if not errors else "partial",
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "etl_summary": {
                "days_processed": len(dates) - len(errors),
                "days_with_errors": len(errors),
                "extracted_records": total_extracted,
                "transformed_records": total_transformed,
                "loaded_records": total_loaded,
                "duration_seconds": round(duration, 2),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "errors": errors or None,
            },
            "message": (
                f"ETL MediaMasterTotals completado para {fecha_inicio} - {fecha_fin}"
                if not errors
                else f"ETL parcial: {len(errors)} día(s) con error de {len(dates)} total"
            ),
        }

    # -------------------------------------------------------------------------
    # STATUS
    # -------------------------------------------------------------------------

    def get_etl_status(self) -> Dict[str, Any]:
        """Estadísticas generales de la tabla auditsa_api_media_master_totals."""
        db = SessionLocal()
        try:
            result = db.query(
                func.count(db_auditsa_media_master_totals.id).label("total_records"),
                func.count(func.distinct(db_auditsa_media_master_totals.IdMaster)).label("unique_masters"),
                func.count(func.distinct(db_auditsa_media_master_totals.Medio)).label("unique_medios"),
                func.count(func.distinct(db_auditsa_media_master_totals.Anunciante)).label("unique_advertisers"),
                func.count(func.distinct(db_auditsa_media_master_totals.Marca)).label("unique_brands"),
                func.min(db_auditsa_media_master_totals.FechaInicio).label("first_fecha"),
                func.max(db_auditsa_media_master_totals.FechaFin).label("last_fecha"),
                func.coalesce(
                    func.sum(db_auditsa_media_master_totals.TotalInversion), 0
                ).label("total_inversion"),
            ).first()

            return {
                "status": "success",
                "data_exists": (result.total_records or 0) > 0,
                "statistics": {
                    "total_records": result.total_records or 0,
                    "unique_masters": result.unique_masters or 0,
                    "unique_medios": result.unique_medios or 0,
                    "unique_advertisers": result.unique_advertisers or 0,
                    "unique_brands": result.unique_brands or 0,
                    "first_fecha": str(result.first_fecha) if result.first_fecha else None,
                    "last_fecha": str(result.last_fecha) if result.last_fecha else None,
                    "total_inversion": float(result.total_inversion or 0),
                },
            }

        except Exception as e:
            self.logger.error(f"❌ Error obteniendo status: {str(e)}")
            return {"status": "error", "error": str(e), "data_exists": False}

        finally:
            db.close()

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------

    def _fix_nulls_in_db(self, db_session: Session) -> None:
        """Normaliza NULLs y strings inválidos tras cada carga."""
        string_cols = [
            "Periodo", "Medio", "Canal", "GEstacion", "FHoraria", "TipoSpot",
            "Genero", "TipoMaster", "Version", "Anunciante", "Marca", "Submarca",
            "Producto", "Segmento", "Industria", "Mercado", "Testigo",
        ]
        int_cols = ["TotalHit"]
        float_cols = ["TotalInversion"]

        for col in string_cols:
            db_session.execute(text(
                f'UPDATE public.auditsa_api_media_master_totals '
                f'SET "{col}" = \'N.A.\' '
                f'WHERE "{col}" IS NULL OR TRIM("{col}") = \'\' '
                f'OR "{col}" IN (\'nan\', \'None\', \'N/A\', \'null\')'
            ))

        for col in int_cols:
            db_session.execute(text(
                f'UPDATE public.auditsa_api_media_master_totals '
                f'SET "{col}" = 0 WHERE "{col}" IS NULL'
            ))

        for col in float_cols:
            db_session.execute(text(
                f'UPDATE public.auditsa_api_media_master_totals '
                f'SET "{col}" = 0.0 WHERE "{col}" IS NULL'
            ))

        db_session.commit()
        self.logger.info(
            "🧹 FIX: NULLs normalizados en auditsa_api_media_master_totals"
        )

    def delete_data_by_range(
        self, fecha_inicio: str, fecha_fin: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Elimina registros cuyo rango de consulta coincide exactamente con
        (FechaInicio, FechaFin).

        Args:
            fecha_inicio: Fecha de inicio YYYYMMDD
            fecha_fin: Fecha de fin YYYYMMDD
            db: Sesión de base de datos (opcional)
        """
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self._validate_fecha_format(fecha_inicio)
            self._validate_fecha_format(fecha_fin)

            self.logger.info(
                f"🗑️ DELETE: Eliminando registros con FechaInicio={fecha_inicio} "
                f"FechaFin={fecha_fin}"
            )

            result = db_session.execute(
                text(
                    'DELETE FROM public.auditsa_api_media_master_totals '
                    'WHERE "FechaInicio" >= TO_DATE(:fi, \'YYYYMMDD\') '
                    'AND "FechaInicio" <= TO_DATE(:ff, \'YYYYMMDD\')'
                ),
                {"fi": fecha_inicio, "ff": fecha_fin},
            )
            deleted = result.rowcount
            db_session.commit()

            self.logger.info(f"🗑️ DELETE: {deleted} registros eliminados")

            return {
                "status": "success",
                "message": f"Eliminados {deleted} registros",
                "records_deleted": deleted,
            }

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ERROR: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()

    def delete_all_data(self, db: Optional[Session] = None) -> Dict[str, Any]:
        """Vacía completamente la tabla (refresco total del catálogo)."""
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            result = db_session.execute(
                text("DELETE FROM public.auditsa_api_media_master_totals")
            )
            deleted = result.rowcount
            db_session.commit()

            self.logger.info(
                f"🗑️ DELETE ALL: {deleted} registros eliminados de "
                f"auditsa_api_media_master_totals"
            )
            return {
                "status": "success",
                "message": f"Eliminados {deleted} registros",
                "records_deleted": deleted,
            }

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ALL ERROR: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()
