"""
Servicio ETL (Extract, Transform, Load) para datos de TV de Auditsa
Maneja la extracción, transformación y carga de datos de televisión
"""

import logging
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import text
from typing import List, Dict, Any, Optional
import pandas as pd

from services.auditsa_api_services import AuditsaApiService
from models.api_auditsa_tv import db_auditsa_tv
from config.database import SessionLocal
from services.sqlserver_write_service import write_to_sqlserver

# Configurar logging específico para ETL
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuditsaTvEtlService:
    """Servicio ETL para datos de TV de Auditsa"""

    def __init__(self):
        self.api_service = AuditsaApiService()
        self.logger = logger

    def _convert_fecha_to_date(self, fecha_str: str) -> datetime.date:
        """
        Convierte string de fecha a objeto date de Python
        Maneja múltiples formatos de fecha de la API

        Args:
            fecha_str: Fecha en formato string (YYYYMMDD, YYYY-MM-DD, etc.)

        Returns:
            Objeto date de Python
        """
        try:
            if not fecha_str or str(fecha_str).strip() in ["", "None", "nan", "NaN"]:
                raise ValueError(f"Fecha vacía o nula: {fecha_str}")

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

            raise ValueError(f"No se pudo convertir fecha: {fecha_str}")

        except Exception as e:
            self.logger.error(f"Error convirtiendo fecha '{fecha_str}': {str(e)}")
            # En lugar de fallar, retornar None para que sea filtrado
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
            raise ValueError(
                "La fecha de inicio debe ser menor o igual a la fecha de fin"
            )

        date_list = []
        current_date = start_date

        while current_date <= end_date:
            date_list.append(current_date.strftime("%Y%m%d"))
            current_date += pd.Timedelta(days=1)

        return date_list

    def extract_tv_data(self, fecha: str) -> List[Dict]:
        """
        Extract: Extrae datos de TV desde la API de Auditsa

        Args:
            fecha: Fecha en formato YYYYMMDD

        Returns:
            Lista de diccionarios con datos crudos de TV
        """
        try:
            self.logger.info(
                f"📥 EXTRACT: Iniciando extracción de datos TV para fecha {fecha}"
            )

            # Validar formato de fecha
            self._validate_fecha_format(fecha)

            # Extraer datos de la API
            raw_data = self.api_service.get_tv_data(fecha)

            if not raw_data:
                self.logger.warning(
                    f"📥 EXTRACT: No se encontraron datos para la fecha {fecha}"
                )
                return []

            self.logger.info(
                f"📥 EXTRACT: Extraídos {len(raw_data)} registros de TV para {fecha}"
            )
            return raw_data

        except Exception as e:
            self.logger.error(f"❌ EXTRACT ERROR: {str(e)}")
            raise

    def transform_tv_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform: Transforma y limpia los datos extraídos

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
        """Aplica transformaciones específicas a los datos"""

        # Limpieza de strings
        string_columns = [
            "Plaza",
            "Localidad",
            "Medio",
            "TipoMedio",
            "Canal",
            "Marca",
            "Submarca",
            "Producto",
            "TipoSpot",
            "SubTipoSpot",
            "TipoCorte",
            "Campania",
            "Corporativo",
            "Anunciante",
            "AgenciaP",
            "CentralMedios",
            "Industria",
            "Mercado",
            "Segmento",
            "Programa",
            "Genero",
            "FHoraria",
            "GComercial",
            "GEstacion",
            "Origen",
            "VerCodigo",
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
                df[col] = df[col].str.slice(0, 255)

        # Transformación de fechas - convertir a objetos date
        if "Fecha" in df.columns:
            self.logger.info("🔄 TRANSFORM: Convirtiendo fechas a objetos Date")

            # Mostrar muestra de fechas originales para debug
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

        # Transformación de campos de tiempo
        time_columns = ["HInicio", "HFinal", "PEnCorte"]
        for col in time_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.slice(0, 50)

        # Subtitulo: asignar N.A. a valores nulos o vacíos
        if "Subtitulo" in df.columns:
            df["Subtitulo"] = df["Subtitulo"].astype(str).str.strip()
            df["Subtitulo"] = df["Subtitulo"].replace(["nan", "None", ""], "N.A.").fillna("N.A.")

        # Conversión de tipos numéricos con manejo de errores
        numeric_columns = {
            "IdPlaza": "Int64",
            "IdLocalidad": "Int64",
            "IdMedio": "Int64",
            "IdTipoMedio": "Int64",
            "IdCanal": "Int64",
            "IdMarca": "Int64",
            "IdSubmarca": "Int64",
            "IdProducto": "Int64",
            "DReal": "Int64",
            "DTeorica": "Int64",
            "IdTipoSpot": "Int64",
            "IdSubTipoSpot": "Int64",
            "IdTipoCorte": "Int64",
            "NoCorte": "Int64",
            "IdCampania": "Int64",
            "IdCorporativo": "Int64",
            "IdAnunciante": "Int64",
            "IdAgenciaP": "Int64",
            "IdCentralMedios": "Int64",
            "IdIndustria": "Int64",
            "IdMercado": "Int64",
            "IdSegmento": "Int64",
            "IdPrograma": "Int64",
            "IdGenero": "Int64",
        }

        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

        # Campos BigInteger
        bigint_columns = ["IdVersion", "IdDeteccion"]
        for col in bigint_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Campo Float
        if "Tarifa" in df.columns:
            df["Tarifa"] = pd.to_numeric(df["Tarifa"], errors="coerce")

        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida la calidad de los datos transformados"""

        original_count = len(df)

        # Filtrar registros con fecha válida (requisito mínimo)
        if "Fecha" in df.columns:
            df = df[df["Fecha"].notna()]

        # Filtrar registros con IdDeteccion válido (clave importante)
        if "IdDeteccion" in df.columns:
            df = df[df["IdDeteccion"].notna()]

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

    def load_tv_data(
        self, transformed_data: List[Dict], db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Load: Carga los datos transformados en la base de datos

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
                f"💾 LOAD: Iniciando carga de {len(transformed_data)} registros en base de datos"
            )

            # Preparar registros para inserción
            records_to_insert = []

            for record in transformed_data:
                tv_record = db_auditsa_tv(
                    Fecha=record.get("Fecha"),
                    IdPlaza=record.get("IdPlaza"),
                    Plaza=record.get("Plaza"),
                    IdLocalidad=record.get("IdLocalidad"),
                    Localidad=record.get("Localidad"),
                    IdMedio=record.get("IdMedio"),
                    Medio=record.get("Medio"),
                    IdTipoMedio=record.get("IdTipoMedio"),
                    TipoMedio=record.get("TipoMedio"),
                    IdCanal=record.get("IdCanal"),
                    Canal=record.get("Canal"),
                    HInicio=record.get("HInicio"),
                    HFinal=record.get("HFinal"),
                    IdMarca=record.get("IdMarca"),
                    Marca=record.get("Marca"),
                    IdSubmarca=record.get("IdSubmarca"),
                    Submarca=record.get("Submarca"),
                    IdProducto=record.get("IdProducto"),
                    Producto=record.get("Producto"),
                    IdVersion=record.get("IdVersion"),
                    Version=record.get("Version"),
                    DReal=record.get("DReal"),
                    DTeorica=record.get("DTeorica"),
                    IdTipoSpot=record.get("IdTipoSpot"),
                    TipoSpot=record.get("TipoSpot"),
                    IdSubTipoSpot=record.get("IdSubTipoSpot"),
                    SubTipoSpot=record.get("SubTipoSpot"),
                    IdTipoCorte=record.get("IdTipoCorte"),
                    TipoCorte=record.get("TipoCorte"),
                    NoCorte=record.get("NoCorte"),
                    PEnCorte=record.get("PEnCorte"),
                    IdCampania=record.get("IdCampania"),
                    Campania=record.get("Campania"),
                    IdCorporativo=record.get("IdCorporativo"),
                    Corporativo=record.get("Corporativo"),
                    IdAnunciante=record.get("IdAnunciante"),
                    Anunciante=record.get("Anunciante"),
                    IdAgenciaP=record.get("IdAgenciaP"),
                    AgenciaP=record.get("AgenciaP"),
                    IdCentralMedios=record.get("IdCentralMedios"),
                    CentralMedios=record.get("CentralMedios"),
                    IdIndustria=record.get("IdIndustria"),
                    Industria=record.get("Industria"),
                    IdMercado=record.get("IdMercado"),
                    Mercado=record.get("Mercado"),
                    IdSegmento=record.get("IdSegmento"),
                    Segmento=record.get("Segmento"),
                    IdDeteccion=record.get("IdDeteccion"),
                    Tarifa=record.get("Tarifa"),
                    Testigo=record.get("Testigo"),
                    IdPrograma=record.get("IdPrograma"),
                    Programa=record.get("Programa"),
                    Subtitulo=record.get("Subtitulo"),
                    IdGenero=record.get("IdGenero"),
                    Genero=record.get("Genero"),
                    FHoraria=record.get("FHoraria"),
                    GComercial=record.get("GComercial"),
                    GEstacion=record.get("GEstacion"),
                    Origen=record.get("Origen"),
                    VerCodigo=record.get("VerCodigo"),
                )
                records_to_insert.append(tv_record)

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
            self.fix_nulls_in_db(db_session)

            self.logger.info(f"💾 LOAD: Completada carga de {total_inserted} registros")

            # Dual-write a SQL Server mxTarifas (no-fatal si falla)
            ss_inserted = 0
            ss_error = None
            try:
                ss_inserted = write_to_sqlserver(transformed_data, "tv")
            except Exception as ss_e:
                ss_error = str(ss_e)
                self.logger.error("SQL Server dual-write TV falló (no-fatal): %s", ss_e)

            return {
                "status": "success",
                "message": f"Datos cargados exitosamente",
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
        Ejecuta el pipeline ETL completo para datos de TV

        Args:
            fecha: Fecha en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado completo del proceso ETL
        """
        start_time = datetime.now()

        try:
            self.logger.info(f"🚀 ETL PIPELINE: Iniciando proceso para fecha {fecha}")

            # EXTRACT
            raw_data = self.extract_tv_data(fecha)

            # TRANSFORM
            transformed_data = self.transform_tv_data(raw_data)

            # LOAD
            load_result = self.load_tv_data(transformed_data, db)

            # Calcular tiempo total
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "fecha": fecha,
                "etl_summary": {
                    "extracted_records": len(raw_data),
                    "transformed_records": len(transformed_data),
                    "loaded_records": load_result["records_inserted"],
                    "duration_seconds": round(duration, 2),
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "message": f"ETL completado exitosamente para fecha {fecha}",
            }

            self.logger.info(f"✅ ETL PIPELINE: Completado en {duration:.2f} segundos")
            return result

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_result = {
                "status": "error",
                "fecha": fecha,
                "error": str(e),
                "duration_seconds": round(duration, 2),
                "message": f"Error en ETL para fecha {fecha}: {str(e)}",
            }

            self.logger.error(f"❌ ETL PIPELINE FAILED: {str(e)}")
            return error_result

    def run_etl_pipeline_range(
        self, fecha_inicio: str, fecha_fin: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Ejecuta el pipeline ETL para un rango de fechas

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            db: Sesión de base de datos (opcional)

        Returns:
            Diccionario con resultado completo del proceso ETL para el rango
        """
        start_time = datetime.now()

        try:
            self.logger.info(
                f"🚀 ETL PIPELINE RANGE: Iniciando proceso para rango {fecha_inicio} - {fecha_fin}"
            )

            # Generar lista de fechas
            date_list = self._generate_date_range(fecha_inicio, fecha_fin)

            total_extracted = 0
            total_transformed = 0
            total_loaded = 0
            successful_dates = []
            failed_dates = []

            # Procesar cada fecha
            for fecha in date_list:
                try:
                    self.logger.info(f"📅 Procesando fecha: {fecha}")

                    # Ejecutar ETL para esta fecha
                    result = self.run_etl_pipeline(fecha, db)

                    if result["status"] == "success":
                        successful_dates.append(fecha)
                        total_extracted += result["etl_summary"]["extracted_records"]
                        total_transformed += result["etl_summary"][
                            "transformed_records"
                        ]
                        total_loaded += result["etl_summary"]["loaded_records"]
                    else:
                        failed_dates.append(
                            {"fecha": fecha, "error": result.get("error")}
                        )

                except Exception as e:
                    self.logger.error(f"❌ Error procesando fecha {fecha}: {str(e)}")
                    failed_dates.append({"fecha": fecha, "error": str(e)})

            # Calcular tiempo total
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success" if len(successful_dates) > 0 else "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "etl_summary": {
                    "total_dates_processed": len(date_list),
                    "successful_dates": len(successful_dates),
                    "failed_dates": len(failed_dates),
                    "total_extracted_records": total_extracted,
                    "total_transformed_records": total_transformed,
                    "total_loaded_records": total_loaded,
                    "duration_seconds": round(duration, 2),
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
                "successful_dates": successful_dates,
                "failed_dates": failed_dates,
                "message": f"ETL de rango completado: {len(successful_dates)} exitosas, {len(failed_dates)} fallidas",
            }

            self.logger.info(
                f"✅ ETL PIPELINE RANGE: Completado en {duration:.2f} segundos"
            )
            return result

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_result = {
                "status": "error",
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "error": str(e),
                "duration_seconds": round(duration, 2),
                "message": f"Error en ETL de rango {fecha_inicio} - {fecha_fin}: {str(e)}",
            }

            self.logger.error(f"❌ ETL PIPELINE RANGE FAILED: {str(e)}")
            return error_result

    def get_etl_status(self, fecha: str) -> Dict[str, Any]:
        """
        Obtiene el estado de datos ETL para una fecha específica

        Args:
            fecha: Fecha en formato YYYYMMDD

        Returns:
            Diccionario con estadísticas de la fecha
        """
        db_session = SessionLocal()

        try:
            # Consultar registros existentes para la fecha
            query = text(
                """
                SELECT COUNT(*) as total_records,
                       MIN("Fecha") as min_fecha,
                       MAX("Fecha") as max_fecha,
                       COUNT(DISTINCT "IdCanal") as unique_channels,
                       COUNT(DISTINCT "IdMarca") as unique_brands,
                       COUNT(DISTINCT "IdAnunciante") as unique_advertisers
                FROM public.auditsa_api_tv
                WHERE "Fecha" = TO_DATE(:fecha, 'YYYYMMDD')
            """
            )

            result = db_session.execute(query, {"fecha": fecha}).fetchone()

            return {
                "fecha": fecha,
                "total_records": result.total_records if result else 0,
                "unique_channels": result.unique_channels if result else 0,
                "unique_brands": result.unique_brands if result else 0,
                "unique_advertisers": result.unique_advertisers if result else 0,
                "data_exists": result.total_records > 0 if result else False,
            }

        except Exception as e:
            self.logger.error(f"Error consultando estado ETL: {str(e)}")
            return {"fecha": fecha, "error": str(e), "data_exists": False}

        finally:
            db_session.close()

    def get_etl_status_range(self, fecha_inicio: str, fecha_fin: str) -> Dict[str, Any]:
        """
        Obtiene el estado de datos ETL para un rango de fechas

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD

        Returns:
            Diccionario con estadísticas del rango de fechas
        """
        db_session = SessionLocal()

        try:
            # Validar fechas
            self._validate_fecha_format(fecha_inicio)
            self._validate_fecha_format(fecha_fin)

            # Consultar registros existentes para el rango
            query = text(
                """
                SELECT COUNT(*) as total_records,
                       MIN("Fecha") as min_fecha,
                       MAX("Fecha") as max_fecha,
                       COUNT(DISTINCT "IdCanal") as unique_channels,
                       COUNT(DISTINCT "IdMarca") as unique_brands,
                       COUNT(DISTINCT "IdAnunciante") as unique_advertisers,
                       COUNT(DISTINCT "Fecha") as unique_dates
                FROM public.auditsa_api_tv
                WHERE "Fecha" >= TO_DATE(:fecha_inicio, 'YYYYMMDD')
                  AND "Fecha" <= TO_DATE(:fecha_fin, 'YYYYMMDD')
            """
            )

            result = db_session.execute(
                query, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
            ).fetchone()

            return {
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "total_records": result.total_records if result else 0,
                "unique_dates": result.unique_dates if result else 0,
                "unique_channels": result.unique_channels if result else 0,
                "unique_brands": result.unique_brands if result else 0,
                "unique_advertisers": result.unique_advertisers if result else 0,
                "data_exists": result.total_records > 0 if result else False,
            }

        except Exception as e:
            self.logger.error(f"Error consultando estado ETL de rango: {str(e)}")
            return {
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "error": str(e),
                "data_exists": False,
            }

        finally:
            db_session.close()

    def fix_nulls_in_db(self, db: Optional[Session] = None) -> Dict[str, Any]:
        """
        Limpia valores inválidos en todas las columnas de auditsa_api_tv:
        - String/Text → reemplaza NULL, '', 'nan', 'None', 'N/A', 'null' por 'N.A.'
        - Integer/BigInteger → reemplaza NULL por 0
        - Float → reemplaza NULL por 0.0
        """
        db_session = db if db else SessionLocal()
        close_session = db is None

        string_cols = [
            "Plaza", "Localidad", "Medio", "TipoMedio", "Canal",
            "HInicio", "HFinal", "Marca", "Submarca", "Producto",
            "Version", "TipoSpot", "SubTipoSpot", "TipoCorte", "PEnCorte",
            "Campania", "Corporativo", "Anunciante", "AgenciaP", "CentralMedios",
            "Industria", "Mercado", "Segmento", "Testigo", "Programa",
            "Subtitulo", "Genero", "FHoraria", "GComercial", "GEstacion",
            "Origen", "VerCodigo",
        ]
        int_cols = [
            "IdPlaza", "IdLocalidad", "IdMedio", "IdTipoMedio", "IdCanal",
            "IdMarca", "IdSubmarca", "IdProducto", "IdVersion",
            "DReal", "DTeorica", "IdTipoSpot", "IdSubTipoSpot", "IdTipoCorte",
            "NoCorte", "IdCampania", "IdCorporativo", "IdAnunciante",
            "IdAgenciaP", "IdCentralMedios", "IdIndustria", "IdMercado",
            "IdSegmento", "IdDeteccion", "IdPrograma", "IdGenero",
        ]
        float_cols = ["Tarifa"]

        try:
            total_updated = 0
            for col in string_cols:
                result = db_session.execute(text(
                    f'UPDATE public.auditsa_api_tv SET "{col}" = \'N.A.\' '
                    f'WHERE "{col}" IS NULL OR TRIM("{col}") = \'\' '
                    f'OR "{col}" IN (\'nan\', \'None\', \'N/A\', \'null\')'
                ))
                total_updated += result.rowcount
            for col in int_cols:
                result = db_session.execute(text(
                    f'UPDATE public.auditsa_api_tv SET "{col}" = 0 WHERE "{col}" IS NULL'
                ))
                total_updated += result.rowcount
            for col in float_cols:
                result = db_session.execute(text(
                    f'UPDATE public.auditsa_api_tv SET "{col}" = 0.0 WHERE "{col}" IS NULL'
                ))
                total_updated += result.rowcount

            db_session.commit()
            self.logger.info(f"🧹 FIX: {total_updated} celdas normalizadas en auditsa_api_tv")
            return {
                "status": "success",
                "records_updated": total_updated,
                "message": f"{total_updated} celdas actualizadas en auditsa_api_tv",
            }
        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ FIX NULLS ERROR: {str(e)}")
            raise
        finally:
            if close_session:
                db_session.close()

    def delete_tv_data(
        self, fecha: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Elimina datos de TV para una fecha específica en la base de datos mxRepository.source_temp.auditsa_api
        Args:
            fecha: Fecha en formato YYYYMMDD
            db: Sesión de base de datos (opcional)
        Returns:
            Diccionario con resultado de la operación
        """

        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(f"🗑️ DELETE: Eliminando datos para fecha {fecha}")

            # Validar fecha
            self._validate_fecha_format(fecha)

            # Ejecutar eliminación
            delete_query = text(
                """
                DELETE FROM public.auditsa_api_tv
                WHERE "Fecha" = TO_DATE(:fecha, 'YYYYMMDD')
            """
            )

            result = db_session.execute(delete_query, {"fecha": fecha})
            deleted_count = result.rowcount

            # Confirmar transacción
            db_session.commit()

            self.logger.info(
                f"🗑️ DELETE: Eliminados {deleted_count} registros para fecha {fecha}"
            )

            return {
                "status": "success",
                "message": f"Eliminados {deleted_count} registros para fecha {fecha}",
                "records_deleted": deleted_count,
            }

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE ERROR: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()

    def delete_tv_data_range(
        self, fecha_inicio: str, fecha_fin: str, db: Optional[Session] = None
    ) -> Dict[str, Any]:
        """
        Elimina datos de TV para un rango de fechas en la base de datos mxRepository.source_temp.auditsa_api
        uri
        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            db: Sesión de base de datos (opcional)
        Returns:
            Diccionario con resultado de la operación
        """
        db_session = db if db else SessionLocal()
        close_session = db is None

        try:
            self.logger.info(
                f"🗑️ DELETE RANGE: Eliminando datos para rango {fecha_inicio} - {fecha_fin}"
            )

            # Validar fechas
            self._validate_fecha_format(fecha_inicio)
            self._validate_fecha_format(fecha_fin)

            # Ejecutar eliminación
            delete_query = text(
                """
                DELETE FROM public.auditsa_api_tv
                WHERE "Fecha" >= TO_DATE(:fecha_inicio, 'YYYYMMDD')
                  AND "Fecha" <= TO_DATE(:fecha_fin, 'YYYYMMDD')
            """
            )

            result = db_session.execute(
                delete_query, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}
            )
            deleted_count = result.rowcount

            # Confirmar transacción
            db_session.commit()

            self.logger.info(
                f"🗑️ DELETE RANGE: Eliminados {deleted_count} registros para rango {fecha_inicio} - {fecha_fin}"
            )

            return {
                "status": "success",
                "message": f"Eliminados {deleted_count} registros para rango {fecha_inicio} - {fecha_fin}",
                "records_deleted": deleted_count,
            }

        except Exception as e:
            db_session.rollback()
            self.logger.error(f"❌ DELETE RANGE ERROR: {str(e)}")
            raise

        finally:
            if close_session:
                db_session.close()
