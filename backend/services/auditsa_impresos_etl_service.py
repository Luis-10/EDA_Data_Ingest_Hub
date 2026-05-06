"""
Servicio ETL especializado para datos de Impresos de Auditsa
Maneja Extract, Transform, Load para información de medios impresos
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime, date, timedelta
from sqlalchemy import text
import pandas as pd
import traceback
import re

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import func

from config.database import SessionLocal
from models.api_auditsa_impresos import db_auditsa_impresos
from services.auditsa_api_services import AuditsaApiService
from services.sqlserver_write_service import write_to_sqlserver


class AuditsaImpresosEtlService:
    """Servicio ETL para datos de Impresos de Auditsa"""

    def __init__(self):
        self.logger = logging.getLogger("services.auditsa_impresos_etl_service")
        self.api_service = AuditsaApiService()

        # IDs de medios para Impresos (automático según modelo)
        self.impresos_ids = [1, 2, 4]  # Impresos IDs de Auditsa

    def _convert_fecha_to_date(self, fecha_str: str) -> Optional[date]:
        """
        Convierte string de fecha a objeto date de Python
        Maneja múltiples formatos de fecha de la API

        Args:
            fecha_str: Fecha en formato string (YYYYMMDD, YYYY-MM-DD, etc.)

        Returns: Objeto date de Python o None si no se puede convertir
        """
        if not fecha_str or str(fecha_str).strip() in ["", "None", "nan", "NaN"]:
            return None

        try:
            # Convertir a string y limpiar
            fecha_clean = str(fecha_str).strip()

            # Intentar diferentes formatos de fecha
            date_formats = [
                "%Y%m%d",  # 20251116
                "%Y-%m-%d",  # 2025-11-16
                "%Y-%m-%dT%H:%M:%S",  # 2025-11-16T12:30:45
                "%Y/%m/%d",  # 2025/11/16
                "%d/%m/%Y",  # 16/11/2025
                "%d-%m-%Y",  # 16-11-2025
            ]

            # Si es solo YYYYMMDD extraer los primeros 8 caracteres
            if len(fecha_clean) >= 8 and fecha_clean[:8].isdigit():
                fecha_clean = fecha_clean[:8]

            # Intentar cada formato
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(fecha_clean, fmt).date()
                    return date_obj
                except ValueError:
                    continue

            # Si ningún formato funcionó, intentar parseo inteligente
            # Extraer números de la fecha
            import re

            numbers = re.findall(r"\d+", fecha_clean)

            if len(numbers) >= 3:
                # Asumir YYYY, MM, DD
                year = int(numbers[0])
                month = int(numbers[1])
                day = int(numbers[2])

                # Validar rangos
                if (
                    year >= 1900
                    and year <= 2100
                    and month >= 1
                    and month <= 12
                    and day >= 1
                    and day <= 31
                ):
                    return datetime(year, month, day).date()

            self.logger.warning(f"No se pudo convertir fecha: {fecha_str}")
            return None

        except Exception as e:
            self.logger.error(f"Error convirtiendo fecha '{fecha_str}': {str(e)}")
            return None

    def _validate_fecha_format(self, fecha: str) -> None:
        """Valida el formato de fecha YYYYMMDD"""
        if len(fecha) != 8 or not fecha.isdigit():
            raise ValueError("Fecha debe estar en formato YYYYMMDD (ej: 20251116)")

        # Validar que sea una fecha válida
        try:
            datetime.strptime(fecha, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Fecha inválida: {fecha}")

    def _generate_date_range(self, fecha_inicio: str, fecha_fin: str) -> List[str]:
        """Genera lista de fechas entre fecha_inicio y fecha_fin"""
        self._validate_fecha_format(fecha_inicio)
        self._validate_fecha_format(fecha_fin)

        start_date = datetime.strptime(fecha_inicio, "%Y%m%d")
        end_date = datetime.strptime(fecha_fin, "%Y%m%d")

        if start_date > end_date:
            raise ValueError("fecha_inicio debe ser anterior o igual a fecha_fin")

        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)

        return date_list

    def extract_impresos_data(self, fecha: str) -> List[Dict]:
        """
        Extract: Extrae datos de Impresos desde la API de Auditsa

        Args:
            fecha: Fecha en formato YYYYMMDD

        Returns:
            Lista de diccionarios con datos crudos de Impresos
        """
        try:
            self.logger.info(
                f"📥 EXTRACT: Iniciando extracción de datos Impresos para fecha {fecha}"
            )

            all_impresos_data = []

            # Obtener datos de Impresos (sin filtro por ID específico)
            self.logger.info(f"📥 EXTRACT: Consultando todos los datos de Impresos")

            try:
                impresos_data = self.api_service.get_impresos_data(fecha)
                if impresos_data:
                    all_impresos_data.extend(impresos_data)
                    self.logger.info(
                        f"📥 EXTRACT: {len(impresos_data)} registros de Impresos"
                    )
                else:
                    self.logger.warning(
                        f"📥 EXTRACT: Sin datos de Impresos para fecha {fecha}"
                    )

            except Exception as e:
                self.logger.error(f"❌ EXTRACT ERROR Impresos: {str(e)}")
                # Continuar con el procesamiento aunque haya error

            total_records = len(all_impresos_data)
            self.logger.info(
                f"📥 EXTRACT: Extraídos {total_records} registros totales de Impresos para {fecha}"
            )

            if total_records == 0:
                self.logger.warning("⚠️ EXTRACT: No se obtuvieron datos de Impresos")

            return all_impresos_data

        except Exception as e:
            self.logger.error(f"❌ EXTRACT ERROR: {str(e)}")
            raise

    def transform_impresos_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform: Transforma y limpia los datos extraídos de Impresos

        Args:
            raw_data: Datos crudos de la API

        Returns:
            Lista de diccionarios con datos transformados y validados
        """
        try:
            if not raw_data:
                self.logger.warning("🔄 TRANSFORM: No hay datos para transformar")
                return []

            self.logger.info(
                f"🔄 TRANSFORM: Iniciando transformación de {len(raw_data)} registros"
            )

            # Mostrar muestra de datos crudos para debug
            if raw_data:
                sample_fecha = raw_data[0].get("Fecha", "N/A")
                self.logger.info(
                    f"🔄 TRANSFORM: Muestra de fecha cruda: '{sample_fecha}'"
                )

            # Convertir a DataFrame para facilitar transformaciones
            df = pd.DataFrame(raw_data)

            # Aplicar transformaciones
            df_transformed = self._apply_data_transformations(df)

            # Validar datos
            df_validated = self._validate_data_quality(df_transformed)

            # Convertir de vuelta a lista de diccionarios
            transformed_data = df_validated.to_dict("records")

            self.logger.info(
                f"🔄 TRANSFORM: Transformados {len(transformed_data)} registros válidos de {len(raw_data)} originales"
            )
            return transformed_data

        except Exception as e:
            self.logger.error(f"❌ TRANSFORM ERROR: {str(e)}")
            # En caso de error, retornar lista vacía en lugar de fallar
            self.logger.warning(
                "🔄 TRANSFORM: Retornando lista vacía debido a error en transformación"
            )
            return []

    def _apply_data_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformaciones específicas a los datos de Impresos"""

        # Limpieza de strings
        string_columns = [
            "Medio",
            "Fuente",
            "Autor",
            "Seccion",
            "Pagina",
            "Anunciante",
            "Marca",
            "Submarca",
            "Producto",
            "Sector",
            "Subsector",
            "Categoria",
        ]

        from services.string_normalizer import normalize_dataframe_columns

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Normalizar: quitar acentos, caracteres raros y convertir a MAYÚSCULAS
        normalize_dataframe_columns(df, string_columns)

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].replace(["nan", "None", ""], "N.A.").fillna("N.A.")
                df[col] = df[col].str.slice(0, 128)

        # Transformación de fechas - convertir a objetos date para Impresos
        if "Fecha" in df.columns:
            self.logger.info("🔄 TRANSFORM: Convirtiendo fechas a objetos Date")

            # Muestra de fechas originales para debug
            sample_fechas = df["Fecha"].head(3).tolist()
            self.logger.info(
                f"🔄 TRANSFORM: Muestra fechas originales: {sample_fechas}"
            )

            # Limpiar y convertir fechas
            df["Fecha"] = df["Fecha"].astype(str).str.strip()

            # Aplicar conversión a date usando la función helper
            df["Fecha"] = df["Fecha"].apply(
                lambda x: (
                    self._convert_fecha_to_date(x)
                    if x and str(x).strip() not in ["nan", "None", "", "NaN"]
                    else None
                )
            )

            # Contar fechas convertidas exitosamente y filtrar registros con fechas inválidas
            valid_dates_before = df["Fecha"].notna().sum()
            invalid_dates = df["Fecha"].isna().sum()
            total_records = len(df)

            self.logger.info(
                f"🔄 TRANSFORM: Fechas convertidas: {valid_dates_before}/{total_records}"
            )
            self.logger.info(
                f"🔄 TRANSFORM: Fechas inválidas: {invalid_dates}/{total_records}"
            )

            # Filtrar registros con fechas válidas
            if invalid_dates > 0:
                df = df.dropna(subset=["Fecha"])
                remaining_records = len(df)
                self.logger.warning(
                    f"⚠️ TRANSFORM: Removidos {invalid_dates} registros con fechas inválidas. Restantes: {remaining_records}"
                )

            if len(df) == 0:
                self.logger.error(
                    "❌ TRANSFORM: No quedaron registros con fechas válidas"
                )

        # Convertir campos numéricos
        numeric_fields = ["IdMedio", "IdFuente", "Tiraje", "Costo", "Dimension"]

        for field in numeric_fields:
            if field in df.columns:
                if field in ["Costo", "Dimension"]:
                    df[field] = pd.to_numeric(df[field], errors="coerce")
                    df[field] = df[field].fillna(0.0)
                else:
                    df[field] = pd.to_numeric(df[field], errors="coerce")
                    # IdFuente defaults to 0; IdMedio keeps None (required for validation)
                    if field == "IdFuente":
                        df[field] = df[field].fillna(0).astype(int)
                    elif "Id" in field:
                        df[field] = df[field].where(df[field].notna(), None)
                    else:
                        df[field] = df[field].fillna(0)

        # Manejar campos de texto largo
        text_fields = ["Testigo", "TextoNota"]
        for field in text_fields:
            if field not in df.columns:
                df[field] = "N.A."
            else:
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace(["nan", "None", "NaN", ""], "N.A.").fillna("N.A.")

        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida la calidad de los datos transformados de Impresos"""

        original_count = len(df)

        # Filtrar registros con fecha válida (requisito mínimo)
        if "Fecha" in df.columns:
            df = df[df["Fecha"].notna()]

        # Filtrar registros con al menos un identificador válido
        if "IdMedio" in df.columns:
            df = df[df["IdMedio"].notna()]

        final_count = len(df)
        excluded_count = original_count - final_count

        if excluded_count > 0:
            self.logger.warning(
                f"🔄 VALIDATION: Excluidos {excluded_count} registros por validaciones"
            )

        self.logger.info(
            f"🔄 VALIDATION: Validados {final_count} de {original_count} registros"
        )

        return df

    def _fix_nulls_in_db(self, db_session: Session) -> None:
        """Reemplaza NULLs residuales en auditsa_api_impresos tras cada carga."""
        string_cols = [
            "Medio", "Fuente", "Autor", "Seccion", "Pagina",
            "Anunciante", "Marca", "Submarca", "Producto",
            "Sector", "Subsector", "Categoria", "Testigo", "TextoNota",
        ]
        int_cols   = ["IdMedio", "IdFuente", "Tiraje"]
        float_cols = ["Costo", "Dimension"]

        for col in string_cols:
            db_session.execute(
                text(f'UPDATE public.auditsa_api_impresos SET "{col}" = \'N.A.\' WHERE "{col}" IS NULL OR TRIM("{col}") = \'\'')
            )
        for col in int_cols:
            db_session.execute(
                text(f'UPDATE public.auditsa_api_impresos SET "{col}" = 0 WHERE "{col}" IS NULL')
            )
        for col in float_cols:
            db_session.execute(
                text(f'UPDATE public.auditsa_api_impresos SET "{col}" = 0.0 WHERE "{col}" IS NULL')
            )
        db_session.commit()
        self.logger.info("🧹 LOAD: NULLs normalizados en auditsa_api_impresos")

    def load_impresos_data(
        self, transformed_data: List[Dict], db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Load: Carga los datos transformados de Impresos en la base de datos

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

        # Usar sesión proporcionada o crear nueva
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(
                f"💾 LOAD: Iniciando carga de {len(transformed_data)} registros de Impresos en base de datos"
            )

            # Preparar registros para inserción
            records_to_insert = []

            for record in transformed_data:
                impresos_record = db_auditsa_impresos(
                    IdMedio=record.get("IdMedio"),
                    Medio=record.get("Medio"),
                    IdFuente=record.get("IdFuente") if record.get("IdFuente") is not None else 0,
                    Fuente=record.get("Fuente"),
                    Autor=record.get("Autor"),
                    Seccion=record.get("Seccion"),
                    Pagina=record.get("Pagina"),
                    Tiraje=record.get("Tiraje"),
                    Costo=record.get("Costo"),
                    Fecha=record.get("Fecha"),
                    Testigo=record.get("Testigo") or "N.A.",
                    Anunciante=record.get("Anunciante"),
                    Marca=record.get("Marca"),
                    Submarca=record.get("Submarca"),
                    Producto=record.get("Producto"),
                    Sector=record.get("Sector"),
                    Subsector=record.get("Subsector"),
                    Categoria=record.get("Categoria"),
                    Dimension=record.get("Dimension"),
                    TextoNota=record.get("TextoNota") or "N.A.",
                )
                records_to_insert.append(impresos_record)

            # Inserción en lotes para optimizar rendimiento
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i : i + batch_size]
                db_session.add_all(batch)
                db_session.flush()
                total_inserted += len(batch)

                if total_inserted % 5000 == 0:
                    self.logger.info(
                        f"💾 LOAD: Procesados {total_inserted}/{len(records_to_insert)} registros"
                    )

            # Confirmar transacción
            db_session.commit()

            # Normalizar NULLs residuales directamente en BD
            self._fix_nulls_in_db(db_session)

            self.logger.info(
                f"💾 LOAD: Completada carga de {total_inserted} registros de Impresos"
            )

            # Dual-write a SQL Server mxTarifas (no-fatal si falla)
            ss_inserted = 0
            ss_error = None
            try:
                ss_inserted = write_to_sqlserver(transformed_data, "impresos")
            except Exception as ss_e:
                ss_error = str(ss_e)
                self.logger.error("SQL Server dual-write Impresos falló (no-fatal): %s", ss_e)

            return {
                "status": "success",
                "message": f"Datos de Impresos cargados exitosamente",
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

    def run_etl_pipeline(
        self, fecha: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL completo para datos de Impresos

        Args:
            fecha: Fecha en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado del proceso ETL
        """
        start_time = datetime.now()

        try:
            self._validate_fecha_format(fecha)
            self.logger.info(
                f"🚀 ETL PIPELINE: Iniciando proceso de Impresos para fecha {fecha}"
            )

            # Extract
            raw_data = self.extract_impresos_data(fecha)

            # Transform
            transformed_data = self.transform_impresos_data(raw_data)

            # Load
            load_result = self.load_impresos_data(transformed_data, db)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info(f"✅ ETL PIPELINE: Completado en {duration:.2f} segundos")

            return {
                "status": "success",
                "fecha": fecha,
                "etl_summary": {
                    "extracted_records": len(raw_data),
                    "transformed_records": len(transformed_data),
                    "loaded_records": load_result["records_inserted"],
                    "duration_seconds": duration,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "message": f"ETL de Impresos completado exitosamente para fecha {fecha}",
            }

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_msg = str(e)
            self.logger.error(f"❌ ETL PIPELINE FAILED: {error_msg}")

            return {
                "status": "error",
                "fecha": fecha,
                "error": error_msg,
                "duration_seconds": duration,
                "message": f"Error en ETL de Impresos para fecha {fecha}: {error_msg}",
            }

    def run_etl_pipeline_range(
        self, fecha_inicio: str, fecha_fin: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL para un rango de fechas de Impresos

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado del proceso ETL
        """
        start_time = datetime.now()

        try:
            # Generar lista de fechas
            date_list = self._generate_date_range(fecha_inicio, fecha_fin)
            self.logger.info(
                f"🚀 ETL PIPELINE RANGE: Iniciando proceso de Impresos para rango {fecha_inicio} - {fecha_fin}"
            )
            self.logger.info(f"📅 Total de fechas a procesar: {len(date_list)}")

            total_extracted = 0
            total_transformed = 0
            total_loaded = 0
            successful_dates = []
            failed_dates = []

            # Procesar cada fecha
            for fecha in date_list:
                self.logger.info(f"📅 Procesando fecha: {fecha}")

                try:
                    result = self.run_etl_pipeline(fecha, db)

                    if result["status"] == "success":
                        successful_dates.append(fecha)
                        etl_summary = result["etl_summary"]
                        total_extracted += etl_summary["extracted_records"]
                        total_transformed += etl_summary["transformed_records"]
                        total_loaded += etl_summary["loaded_records"]
                    else:
                        failed_dates.append(fecha)

                except Exception as e:
                    self.logger.error(f"❌ Error procesando fecha {fecha}: {str(e)}")
                    failed_dates.append(fecha)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info(
                f"✅ ETL PIPELINE RANGE: Completado en {duration:.2f} segundos"
            )

            return {
                "status": "success",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "etl_summary": {
                    "total_dates_processed": len(date_list),
                    "successful_dates": len(successful_dates),
                    "failed_dates": len(failed_dates),
                    "total_extracted_records": total_extracted,
                    "total_transformed_records": total_transformed,
                    "total_loaded_records": total_loaded,
                    "duration_seconds": duration,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "successful_dates": successful_dates,
                "failed_dates": failed_dates,
                "message": f"ETL de rango de Impresos completado: {len(successful_dates)} exitosas, {len(failed_dates)} fallidas",
            }

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_msg = str(e)
            self.logger.error(f"❌ ETL PIPELINE RANGE FAILED: {error_msg}")

            return {
                "status": "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "error": error_msg,
                "duration_seconds": duration,
                "message": f"Error en ETL de rango de Impresos {fecha_inicio}-{fecha_fin}: {error_msg}",
            }

    def get_etl_status(self, fecha: str) -> Dict[str, Any]:
        """
        Obtiene el estado de los datos de Impresos para una fecha específica

        Args:
            fecha: Fecha en formato YYYYMMDD

        Returns:
            Diccionario con estadísticas de la fecha
        """
        try:
            self._validate_fecha_format(fecha)

            # Convertir fecha a formato date object
            fecha_obj = datetime.strptime(fecha, "%Y%m%d").date()

            db = SessionLocal()

            # Consultar estadísticas
            result = (
                db.query(
                    func.count(db_auditsa_impresos.id).label("total_records"),
                    func.count(func.distinct(db_auditsa_impresos.IdFuente)).label(
                        "unique_sources"
                    ),
                    func.count(func.distinct(db_auditsa_impresos.Marca)).label(
                        "unique_brands"
                    ),
                    func.count(func.distinct(db_auditsa_impresos.Anunciante)).label(
                        "unique_advertisers"
                    ),
                )
                .filter(db_auditsa_impresos.Fecha == fecha_obj)
                .first()
            )

            db.close()

            return {
                "status": "success",
                "fecha": fecha,
                "data_exists": result.total_records > 0 if result else False,
                "statistics": {
                    "total_records": result.total_records if result else 0,
                    "unique_sources": result.unique_sources if result else 0,
                    "unique_brands": result.unique_brands if result else 0,
                    "unique_advertisers": result.unique_advertisers if result else 0,
                },
                "message": f"Estadísticas para fecha {fecha}",
            }

        except Exception as e:
            self.logger.error(f"❌ Error obteniendo status: {str(e)}")
            return {
                "status": "error",
                "fecha": fecha,
                "error": str(e),
                "message": f"Error obteniendo estadísticas para fecha {fecha}",
            }

    def get_etl_status_range(self, fecha_inicio: str, fecha_fin: str) -> Dict[str, Any]:
        """
        Obtiene el estado de los datos de Impresos para un rango de fechas

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD

        Returns:
            Diccionario con estadísticas del rango
        """
        try:
            date_list = self._generate_date_range(fecha_inicio, fecha_fin)

            # Convertir fechas a objetos date
            fecha_inicio_obj = datetime.strptime(fecha_inicio, "%Y%m%d").date()
            fecha_fin_obj = datetime.strptime(fecha_fin, "%Y%m%d").date()

            db = SessionLocal()

            # Consultar estadísticas del rango
            result = (
                db.query(
                    func.count(db_auditsa_impresos.id).label("total_records"),
                    func.count(func.distinct(db_auditsa_impresos.IdFuente)).label(
                        "unique_sources"
                    ),
                    func.count(func.distinct(db_auditsa_impresos.Marca)).label(
                        "unique_brands"
                    ),
                    func.count(func.distinct(db_auditsa_impresos.Anunciante)).label(
                        "unique_advertisers"
                    ),
                    func.count(func.distinct(db_auditsa_impresos.Fecha)).label(
                        "unique_dates"
                    ),
                )
                .filter(
                    db_auditsa_impresos.Fecha >= fecha_inicio_obj,
                    db_auditsa_impresos.Fecha <= fecha_fin_obj,
                )
                .first()
            )

            db.close()

            return {
                "status": "success",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "total_dates_in_range": len(date_list),
                "data_exists": result.total_records > 0 if result else False,
                "statistics": {
                    "total_records": result.total_records if result else 0,
                    "unique_dates": result.unique_dates if result else 0,
                    "unique_sources": result.unique_sources if result else 0,
                    "unique_brands": result.unique_brands if result else 0,
                    "unique_advertisers": result.unique_advertisers if result else 0,
                },
                "message": f"Estadísticas para rango {fecha_inicio} - {fecha_fin}",
            }

        except Exception as e:
            self.logger.error(f"❌ Error obteniendo status de rango: {str(e)}")
            return {
                "status": "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "error": str(e),
                "message": f"Error obteniendo estadísticas para rango {fecha_inicio} - {fecha_fin}",
            }

    def delete_impresos_data(
        self, fecha: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Elimina datos de Impresos para una fecha específica en la base de datos mxRepository.source_temp.auditsa_api_impresos

        Args:
            fecha: Fecha en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado de la operación
        """
        self._validate_fecha_format(fecha)

        # Usar sesión proporcionada o crear nueva
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(
                f"🗑️ DELETE: Iniciando eliminación de datos de Impresos para fecha {fecha}"
            )

            # Ejecutar eliminación
            delete_query = text(
                """
                DELETE FROM public.auditsa_api_impresos
                WHERE "Fecha" = TO_DATE(:fecha, 'YYYYMMDD')
            """
            )

            result = db_session.execute(delete_query, {"fecha": fecha})
            deleted_count = result.rowcount

            db_session.commit()

            self.logger.info(
                f"🗑️ DELETE: Completada eliminación de {deleted_count} registros de Impresos para fecha {fecha}"
            )

            return {
                "status": "success",
                "message": f"Eliminados {deleted_count} registros de Impresos para fecha {fecha}",
                "records_deleted": deleted_count,
            }

        except SQLAlchemyError as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ERROR - Base de datos: {str(e)}")
            raise

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ERROR - General: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()

    def delete_impresos_data_range(
        self, fecha_inicio: str, fecha_fin: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Elimina datos de Impresos para un rango de fechas en la base de datos mxRepository.source_temp.auditsa_api_impresos

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado de la operación
        """
        self._validate_fecha_format(fecha_inicio)
        self._validate_fecha_format(fecha_fin)

        # Usar sesión proporcionada o crear nueva
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(
                f"🗑️ DELETE RANGE: Iniciando eliminación de datos de Impresos para rango {fecha_inicio} - {fecha_fin}"
            )

            # Ejecutar eliminación
            delete_query = text(
                """
                DELETE FROM public.auditsa_api_impresos
                WHERE "Fecha" >= TO_DATE(:fecha_inicio, 'YYYYMMDD')
                  AND "Fecha" <= TO_DATE(:fecha_fin, 'YYYYMMDD')
            """
            )

            result = db_session.execute(
                delete_query, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
            )
            deleted_count = result.rowcount

            db_session.commit()

            self.logger.info(
                f"🗑️ DELETE RANGE: Completada eliminación de {deleted_count} registros de Impresos para rango {fecha_inicio} - {fecha_fin}"
            )

            return {
                "status": "success",
                "message": f"Eliminados {deleted_count} registros de Impresos para rango {fecha_inicio} - {fecha_fin}",
                "records_deleted": deleted_count,
            }

        except SQLAlchemyError as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE RANGE ERROR - Base de datos: {str(e)}")
            raise

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE RANGE ERROR - General: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()
