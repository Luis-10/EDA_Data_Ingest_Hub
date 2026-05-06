"""
Servicio ETL especializado para datos del maestro de medios de Auditsa (GetMediaMasterLibrary)
Maneja Extract, Transform, Load para el catálogo de versiones/spots
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import text, func

from config.database import SessionLocal
from models.api_auditsa_media_master_library import db_auditsa_media_master_library
from services.auditsa_api_services import AuditsaApiService
from services.sqlserver_write_service import write_to_sqlserver


class AuditsaMediaMasterLibraryEtlService:
    """Servicio ETL para el catálogo de maestro de medios de Auditsa"""

    def __init__(self):
        self.logger = logging.getLogger("services.auditsa_media_master_library_etl_service")
        self.api_service = AuditsaApiService()

    def _validate_fecha_format(self, fecha: str) -> None:
        """Valida el formato de fecha YYYYMMDD"""
        if len(fecha) != 8 or not fecha.isdigit():
            raise ValueError("Fecha debe estar en formato YYYYMMDD (ej: 20260201)")
        try:
            datetime.strptime(fecha, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Fecha inválida: {fecha}")

    # -------------------------------------------------------------------------
    # EXTRACT
    # -------------------------------------------------------------------------

    def extract_data(self, fecha_inicio: str, fecha_fin: str) -> List[Dict]:
        """
        Extract: Consulta la API GetMediaMasterLibrary para el rango dado.

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD

        Returns:
            Lista de diccionarios con datos crudos del catálogo
        """
        try:
            self._validate_fecha_format(fecha_inicio)
            self._validate_fecha_format(fecha_fin)

            self.logger.info(
                f"📥 EXTRACT: Consultando MediaMasterLibrary {fecha_inicio} - {fecha_fin}"
            )

            raw_data = self.api_service.get_media_master_library_data(fecha_inicio, fecha_fin)

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

    def transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform: Limpia y valida los datos extraídos del catálogo.

        Args:
            raw_data: Datos crudos de la API

        Returns:
            Lista de diccionarios transformados y validados
        """
        try:
            if not raw_data:
                self.logger.warning("🔄 TRANSFORM: No hay datos para transformar")
                return []

            self.logger.info(
                f"🔄 TRANSFORM: Iniciando transformación de {len(raw_data)} registros"
            )

            df = pd.DataFrame(raw_data)

            df = self._apply_data_transformations(df)
            df = self._validate_data_quality(df)

            transformed_data = df.to_dict("records")

            self.logger.info(
                f"🔄 TRANSFORM: {len(transformed_data)} registros válidos de {len(raw_data)} originales"
            )
            return transformed_data

        except Exception as e:
            self.logger.error(f"❌ TRANSFORM ERROR: {str(e)}")
            self.logger.warning("🔄 TRANSFORM: Retornando lista vacía por error en transformación")
            return []

    def _apply_data_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformaciones y limpieza específicas al catálogo de medios"""

        from services.string_normalizer import normalize_dataframe_columns

        string_columns = [
            "TipoMaster",
            "Anunciante",
            "Marca",
            "Submarca",
            "Producto",
            "Segmento",
            "Industria",
            "Mercado",
        ]

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        normalize_dataframe_columns(df, string_columns)

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
                df[col] = df[col].str.slice(0, 255)

        # Version: texto largo, solo limpiar blancos y nulos
        if "Version" in df.columns:
            df["Version"] = df["Version"].astype(str).str.strip()
            df["Version"] = df["Version"].replace(["nan", "None", ""], "N.A.").fillna("N.A.")

        # Testigo: URL larga, preservar tal cual; solo normalizar nulos
        if "Testigo" in df.columns:
            df["Testigo"] = df["Testigo"].astype(str).str.strip()
            df["Testigo"] = df["Testigo"].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
        else:
            df["Testigo"] = "N.A."

        # Campos numéricos
        if "IdMaster" in df.columns:
            df["IdMaster"] = pd.to_numeric(df["IdMaster"], errors="coerce").astype("Int64")

        if "DTeorica" in df.columns:
            df["DTeorica"] = pd.to_numeric(df["DTeorica"], errors="coerce").fillna(0).astype(int)

        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra registros sin IdMaster válido"""

        original_count = len(df)

        if "IdMaster" in df.columns:
            df = df[df["IdMaster"].notna()]

        final_count = len(df)
        excluded = original_count - final_count

        if excluded > 0:
            self.logger.warning(
                f"🔄 VALIDATION: Excluidos {excluded} registros sin IdMaster válido"
            )

        self.logger.info(
            f"🔄 VALIDATION: {final_count} de {original_count} registros validados"
        )
        return df

    # -------------------------------------------------------------------------
    # LOAD
    # -------------------------------------------------------------------------

    def load_data(
        self, transformed_data: List[Dict], db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Load: Inserta los registros transformados en la base de datos.

        Args:
            transformed_data: Datos transformados y validados
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado de la operación
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
                f"💾 LOAD: Insertando {len(transformed_data)} registros en auditsa_api_media_master_library"
            )

            records_to_insert = [
                db_auditsa_media_master_library(
                    IdMaster=record.get("IdMaster"),
                    TipoMaster=record.get("TipoMaster"),
                    Version=record.get("Version"),
                    Anunciante=record.get("Anunciante"),
                    Marca=record.get("Marca"),
                    Submarca=record.get("Submarca"),
                    Producto=record.get("Producto"),
                    Segmento=record.get("Segmento"),
                    Industria=record.get("Industria"),
                    Mercado=record.get("Mercado"),
                    DTeorica=record.get("DTeorica"),
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
                        f"💾 LOAD: {total_inserted}/{len(records_to_insert)} registros procesados"
                    )

            db_session.commit()

            self._fix_nulls_in_db(db_session)

            self.logger.info(f"💾 LOAD: Carga completada — {total_inserted} registros")

            # Dual-write a SQL Server mxTarifas (no-fatal si falla)
            ss_inserted = 0
            ss_error = None
            try:
                ss_inserted = write_to_sqlserver(transformed_data, "media_master_library")
            except Exception as ss_e:
                ss_error = str(ss_e)
                self.logger.error("SQL Server dual-write MediaMasterLibrary falló (no-fatal): %s", ss_e)

            return {
                "status": "success",
                "message": "Datos de MediaMasterLibrary cargados exitosamente",
                "records_inserted": total_inserted,
                "sqlserver": {"inserted": ss_inserted, "error": ss_error},
            }

        except IntegrityError as e:
            db_session.rollback()
            self.logger.error(f"❌ LOAD ERROR - Integridad: {str(e)}")
            raise SQLAlchemyError(f"Error de integridad en base de datos: {str(e)}")

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

    def run_etl_pipeline(
        self,
        fecha_inicio: str,
        fecha_fin: str,
        db: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL completo para el rango de fechas indicado.

        A diferencia de TV/Radio/Impresos, este endpoint acepta un rango directo,
        por lo que se realiza una sola llamada a la API por ejecución.

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado completo del proceso ETL
        """
        start_time = datetime.now()

        try:
            self.logger.info(
                f"🚀 ETL PIPELINE: Iniciando MediaMasterLibrary {fecha_inicio} - {fecha_fin}"
            )

            raw_data = self.extract_data(fecha_inicio, fecha_fin)
            transformed_data = self.transform_data(raw_data)
            load_result = self.load_data(transformed_data, db)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info(f"✅ ETL PIPELINE: Completado en {duration:.2f} segundos")

            return {
                "status": "success",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "etl_summary": {
                    "extracted_records": len(raw_data),
                    "transformed_records": len(transformed_data),
                    "loaded_records": load_result["records_inserted"],
                    "duration_seconds": round(duration, 2),
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "message": f"ETL MediaMasterLibrary completado para {fecha_inicio} - {fecha_fin}",
            }

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.error(f"❌ ETL PIPELINE FAILED: {str(e)}")

            return {
                "status": "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "error": str(e),
                "duration_seconds": round(duration, 2),
                "message": f"Error en ETL MediaMasterLibrary {fecha_inicio} - {fecha_fin}: {str(e)}",
            }

    # -------------------------------------------------------------------------
    # STATUS
    # -------------------------------------------------------------------------

    def get_etl_status(self) -> Dict[str, Any]:
        """
        Retorna estadísticas generales de la tabla auditsa_api_media_master_library.

        Returns:
            Diccionario con totales y conteos distintos
        """
        db = SessionLocal()
        try:
            result = db.query(
                func.count(db_auditsa_media_master_library.id).label("total_records"),
                func.count(func.distinct(db_auditsa_media_master_library.IdMaster)).label("unique_masters"),
                func.count(func.distinct(db_auditsa_media_master_library.TipoMaster)).label("unique_types"),
                func.count(func.distinct(db_auditsa_media_master_library.Anunciante)).label("unique_advertisers"),
                func.count(func.distinct(db_auditsa_media_master_library.Marca)).label("unique_brands"),
            ).first()

            return {
                "status": "success",
                "data_exists": result.total_records > 0 if result else False,
                "statistics": {
                    "total_records": result.total_records if result else 0,
                    "unique_masters": result.unique_masters if result else 0,
                    "unique_types": result.unique_types if result else 0,
                    "unique_advertisers": result.unique_advertisers if result else 0,
                    "unique_brands": result.unique_brands if result else 0,
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
        """Normaliza NULLs residuales tras cada carga."""

        string_cols = [
            "TipoMaster", "Version", "Anunciante", "Marca", "Submarca",
            "Producto", "Segmento", "Industria", "Mercado", "Testigo",
        ]
        int_cols = ["DTeorica"]

        for col in string_cols:
            db_session.execute(text(
                f'UPDATE public.auditsa_api_media_master_library '
                f'SET "{col}" = \'N.A.\' '
                f'WHERE "{col}" IS NULL OR TRIM("{col}") = \'\' '
                f'OR "{col}" IN (\'nan\', \'None\', \'N/A\', \'null\')'
            ))

        for col in int_cols:
            db_session.execute(text(
                f'UPDATE public.auditsa_api_media_master_library '
                f'SET "{col}" = 0 WHERE "{col}" IS NULL'
            ))

        db_session.commit()
        self.logger.info("🧹 FIX: NULLs normalizados en auditsa_api_media_master_library")

    def delete_all_data(self, db: Optional[Session] = None) -> Dict[str, Any]:
        """
        Elimina todos los registros de la tabla (útil para refrescar el catálogo completo).

        Args:
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado de la operación
        """
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info("🗑️ DELETE: Vaciando tabla auditsa_api_media_master_library")

            result = db_session.execute(
                text("DELETE FROM public.auditsa_api_media_master_library")
            )
            deleted_count = result.rowcount
            db_session.commit()

            self.logger.info(f"🗑️ DELETE: {deleted_count} registros eliminados")

            return {
                "status": "success",
                "message": f"Eliminados {deleted_count} registros",
                "records_deleted": deleted_count,
            }

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ERROR: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()
