"""
Servicio de procesamiento de datos de Auditsa usando Polars
Optimizado para manejo eficiente de grandes volúmenes de datos
"""

import polars as pl
from typing import List, Dict, Optional
import logging
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import text
from services.string_normalizer import normalize_str

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuditsaDataProcessor:
    """Procesador de datos de Auditsa optimizado con Polars"""

    def __init__(self):
        self.logger = logger

        # Schema para validación de datos TV
        self.tv_schema = {
            "Fecha": pl.String,
            "IdPlaza": pl.Int32,
            "Plaza": pl.String,
            "IdLocalidad": pl.Int32,
            "Localidad": pl.String,
            "IdMedio": pl.Int32,
            "Medio": pl.String,
            "IdTipoMedio": pl.Int32,
            "TipoMedio": pl.String,
            "IdCanal": pl.Int32,
            "Canal": pl.String,
            "HInicio": pl.String,
            "HFinal": pl.String,
            "IdMarca": pl.Int32,
            "Marca": pl.String,
            "IdSubmarca": pl.Int32,
            "Submarca": pl.String,
            "IdProducto": pl.Int32,
            "Producto": pl.String,
            "IdVersion": pl.Int64,
            "Version": pl.String,
            "DReal": pl.Int32,
            "DTeorica": pl.Int32,
            "IdTipoSpot": pl.Int32,
            "TipoSpot": pl.String,
            "IdSubTipoSpot": pl.Int32,
            "SubTipoSpot": pl.String,
            "IdTipoCorte": pl.Int32,
            "TipoCorte": pl.String,
            "NoCorte": pl.Int32,
            "PEnCorte": pl.String,
            "IdCampania": pl.Int32,
            "Campania": pl.String,
            "IdCorporativo": pl.Int32,
            "Corporativo": pl.String,
            "IdAnunciante": pl.Int32,
            "Anunciante": pl.String,
            "IdAgenciaP": pl.Int32,
            "AgenciaP": pl.String,
            "IdCentralMedios": pl.Int32,
            "CentralMedios": pl.String,
            "IdIndustria": pl.Int32,
            "Industria": pl.String,
            "IdMercado": pl.Int32,
            "Mercado": pl.String,
            "IdSegmento": pl.Int32,
            "Segmento": pl.String,
            "IdDeteccion": pl.Int64,
            "Tarifa": pl.Float64,
            "Testigo": pl.String,
            "IdPrograma": pl.Int32,
            "Programa": pl.String,
            "Subtitulo": pl.String,
            "IdGenero": pl.Int32,
            "Genero": pl.String,
            "FHoraria": pl.String,
            "GComercial": pl.String,
            "GEstacion": pl.String,
            "Origen": pl.String,
            "VerCodigo": pl.String,
        }

    def process_tv_data(self, raw_data: List[Dict]) -> pl.DataFrame:
        """
        Procesa datos de TV con validaciones y transformaciones usando Polars
        
        Args: 
            raw_data: Lista de diccionarios con datos crudos de la API
        
        Returns: DataFrame de Polars procesado y validado
        """
        try:
            if not raw_data:
                self.logger.warning("No hay datos para procesar")
                return pl.DataFrame()

            self.logger.info(f"Procesando {len(raw_data)} registros de TV")

            # 1. Crear DataFrame desde datos crudos
            df = pl.DataFrame(raw_data)

            # Lista de columnas que coinciden con nuestro modelo de TV
            expected_columns = [
                "Fecha",
                "IdPlaza",
                "Plaza",
                "IdLocalidad",
                "Localidad",
                "IdMedio",
                "Medio",
                "IdTipoMedio",
                "TipoMedio",
                "IdCanal",
                "Canal",
                "HInicio",
                "HFinal",
                "IdMarca",
                "Marca",
                "IdSubmarca",
                "Submarca",
                "IdProducto",
                "Producto",
                "IdVersion",
                "Version",
                "DReal",
                "DTeorica",
                "IdTipoSpot",
                "TipoSpot",
                "IdSubTipoSpot",
                "SubTipoSpot",
                "IdTipoCorte",
                "TipoCorte",
                "NoCorte",
                "PEnCorte",
                "IdCampania",
                "Campania",
                "IdCorporativo",
                "Corporativo",
                "IdAnunciante",
                "Anunciante",
                "IdAgenciaP",
                "AgenciaP",
                "IdCentralMedios",
                "CentralMedios",
                "IdIndustria",
                "Industria",
                "IdMercado",
                "Mercado",
                "IdSegmento",
                "Segmento",
                "IdDeteccion",
                "Tarifa",
                "Testigo",
                "IdPrograma",
                "Programa",
                "Subtitulo",
                "IdGenero",
                "Genero",
                "FHoraria",
                "GComercial",
                "GEstacion",
                "Origen",
                "VerCodigo",
            ]

            # Filtrar solo las columnas que existen y necesitamos
            available_columns = [col for col in expected_columns if col in df.columns]
            missing_columns = [col for col in expected_columns if col not in df.columns]

            if missing_columns:
                self.logger.warning(
                    f"Columnas faltantes en los datos: {missing_columns}"
                )

            # Seleccionar solo las columnas disponibles
            df = df.select(available_columns)

            # Helper: strip + normalize (quitar acentos, mayúsculas, sin caracteres raros)
            def _norm_col(col_name, max_len=255):
                return (
                    pl.col(col_name)
                    .str.strip_chars()
                    .map_elements(lambda v: normalize_str(v) if v else v, return_dtype=pl.String)
                    .str.slice(0, max_len)
                )

            # 2. Aplicar transformaciones y validaciones
            df_processed = df.with_columns(
                [
                    # Validar y procesar fecha (puede venir en formato ISO o YYYYMMDD)
                    pl.when(pl.col("Fecha").str.len_chars() >= 8)
                    .then(
                        pl.when(pl.col("Fecha").str.contains("T"))
                        .then(pl.col("Fecha").str.slice(0, 10))
                        .otherwise(pl.col("Fecha"))
                    )
                    .otherwise(None)
                    .alias("Fecha"),
                    # Conversiones de tipo con manejo de errores
                    pl.col("IdPlaza").cast(pl.Int32, strict=False),
                    pl.col("IdLocalidad").cast(pl.Int32, strict=False),
                    pl.col("IdMedio").cast(pl.Int32, strict=False),
                    pl.col("IdTipoMedio").cast(pl.Int32, strict=False),
                    pl.col("IdCanal").cast(pl.Int32, strict=False),
                    pl.col("IdMarca").cast(pl.Int32, strict=False),
                    pl.col("IdSubmarca").cast(pl.Int32, strict=False),
                    pl.col("IdProducto").cast(pl.Int32, strict=False),
                    pl.col("IdVersion").cast(pl.Int64, strict=False),
                    pl.col("DReal").cast(pl.Int32, strict=False),
                    pl.col("DTeorica").cast(pl.Int32, strict=False),
                    pl.col("IdTipoSpot").cast(pl.Int32, strict=False),
                    pl.col("IdSubTipoSpot").cast(pl.Int32, strict=False),
                    pl.col("IdTipoCorte").cast(pl.Int32, strict=False),
                    pl.col("NoCorte").cast(pl.Int32, strict=False),
                    pl.col("IdCampania").cast(pl.Int32, strict=False),
                    pl.col("IdCorporativo").cast(pl.Int32, strict=False),
                    pl.col("IdAnunciante").cast(pl.Int32, strict=False),
                    pl.col("IdAgenciaP").cast(pl.Int32, strict=False),
                    pl.col("IdCentralMedios").cast(pl.Int32, strict=False),
                    pl.col("IdIndustria").cast(pl.Int32, strict=False),
                    pl.col("IdMercado").cast(pl.Int32, strict=False),
                    pl.col("IdSegmento").cast(pl.Int32, strict=False),
                    pl.col("IdDeteccion").cast(pl.Int64, strict=False),
                    pl.col("Tarifa").cast(pl.Float64, strict=False),
                    pl.col("IdPrograma").cast(pl.Int32, strict=False),
                    pl.col("IdGenero").cast(pl.Int32, strict=False),
                    # Limpiar y normalizar strings (sin acentos, MAYÚSCULAS, sin caracteres raros)
                    _norm_col("Plaza"),
                    _norm_col("Localidad"),
                    _norm_col("Medio"),
                    _norm_col("TipoMedio"),
                    _norm_col("Canal"),
                    _norm_col("Marca"),
                    _norm_col("Submarca"),
                    _norm_col("Producto"),
                    _norm_col("Version", max_len=9999),
                    _norm_col("TipoSpot"),
                    _norm_col("SubTipoSpot"),
                    _norm_col("TipoCorte"),
                    _norm_col("Campania"),
                    _norm_col("Corporativo"),
                    _norm_col("Anunciante"),
                    _norm_col("AgenciaP"),
                    _norm_col("CentralMedios"),
                    _norm_col("Industria"),
                    _norm_col("Mercado"),
                    _norm_col("Segmento"),
                    _norm_col("Programa"),
                    _norm_col("Genero"),
                    # Campos de tiempo — normalizar también
                    _norm_col("HInicio"),
                    _norm_col("HFinal"),
                    _norm_col("PEnCorte"),
                    _norm_col("FHoraria"),
                    _norm_col("GComercial"),
                    _norm_col("GEstacion"),
                    _norm_col("Origen"),
                    _norm_col("VerCodigo"),
                    # Campos de texto largo
                    _norm_col("Testigo", max_len=9999),
                    _norm_col("Subtitulo", max_len=9999),
                ]
            )

            # 3. Filtrar registros válidos (debe tener fecha válida mínimo)
            df_filtered = df_processed.filter(
                pl.col("Fecha").is_not_null() & (pl.col("Fecha") != "")
            )

            # 4. Estadísticas de validación
            original_count = len(df)
            final_count = len(df_filtered)
            excluded_count = original_count - final_count

            if excluded_count > 0:
                self.logger.warning(
                    f"Se excluyeron {excluded_count} registros por validaciones"
                )

            self.logger.info(
                f"Procesados {final_count} registros válidos de {original_count} totales"
            )

            return df_filtered

        except Exception as e:
            self.logger.error(f"Error procesando datos de TV: {str(e)}")
            raise

    def bulk_insert_tv_data(
        self, df: pl.DataFrame, session: Session, table_name: str = "auditsa_api"
    ) -> int:
        """
        Inserta datos de TV en lote usando SQL directo para máximo rendimiento

        Args:
            df: DataFrame de Polars con datos procesados
            session: Sesión de SQLAlchemy
            table_name: Nombre de la tabla destino

        Returns: Número de registros insertados
        """
        try:
            if df.is_empty():
                self.logger.info("No hay datos para insertar")
                return 0

            records_count = len(df)
            self.logger.info(
                f"Iniciando inserción masiva de {records_count} registros en {table_name}"
            )

            # Convertir a lista de diccionarios para inserción
            records = df.to_dicts()
            
            # Convertir campos Fecha de string a date objects para SQL Server
            for record in records:
                if 'Fecha' in record and record['Fecha']:
                    fecha_str = record['Fecha']
                    try:
                        # Convertir string YYYY-MM-DD a date object
                        # isinstance(): verifica que sea string y tenga longitud suficiente
                        if isinstance(fecha_str, str) and len(fecha_str) >= 10:
                            year = int(fecha_str[0:4])
                            month = int(fecha_str[5:7])
                            day = int(fecha_str[8:10])
                            record['Fecha'] = date(year, month, day)
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Error convirtiendo fecha '{fecha_str}': {e}")
                        record['Fecha'] = None

            # SQL de inserción masiva usando VALUES
            columns = list(df.columns)
            columns_str = ", ".join([f"[{col}]" for col in columns])
            placeholders = ", ".join([f":{col}" for col in columns])

            insert_sql = text(
                f"""
                INSERT INTO [mxRepository].[source_temp].[{table_name}] 
                ({columns_str})
                VALUES ({placeholders})
            """
            )

            # Ejecutar inserción en lotes para mejor rendimiento
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                session.execute(insert_sql, batch)
                total_inserted += len(batch)

                if total_inserted % 5000 == 0:  # Log cada 5000 registros
                    self.logger.info(
                        f"Insertados {total_inserted}/{records_count} registros..."
                    )

            session.commit()
            self.logger.info(
                f"Inserción completada: {total_inserted} registros en {table_name}"
            )

            return total_inserted

        except Exception as e:
            self.logger.error(f"Error en inserción masiva: {str(e)}")
            session.rollback()
            raise

    def bulk_insert_data(
        self,
        df: pl.DataFrame,
        session: Session,
        table_name: str,
        schema: str = "mxRepository.source_temp",
    ) -> int:
        """
        Método genérico para inserción masiva en cualquier tabla

        Args:
            df: DataFrame de Polars con datos procesados
            session: Sesión de SQLAlchemy
            table_name: Nombre de la tabla destino
            schema: Esquema de la base de datos

        Returns: Número de registros insertados
        """
        try:
            if df.is_empty():
                self.logger.info("No hay datos para insertar")
                return 0

            records_count = len(df)
            self.logger.info(
                f"Iniciando inserción masiva de {records_count} registros en {schema}.{table_name}"
            )

            # Convertir a lista de diccionarios para inserción
            records = df.to_dicts()
            
            # Convertir campos Fecha de string a date objects para SQL Server
            for record in records:
                if 'Fecha' in record and record['Fecha']:
                    fecha_str = record['Fecha']
                    try:
                        # Convertir string YYYY-MM-DD a date object
                        if isinstance(fecha_str, str) and len(fecha_str) >= 10:
                            year = int(fecha_str[0:4])
                            month = int(fecha_str[5:7])
                            day = int(fecha_str[8:10])
                            record['Fecha'] = date(year, month, day)
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Error convirtiendo fecha '{fecha_str}': {e}")
                        record['Fecha'] = None

            # SQL de inserción masiva usando VALUES
            columns = list(df.columns)
            columns_str = ", ".join([f"[{col}]" for col in columns])
            placeholders = ", ".join([f":{col}" for col in columns])

            # Construir SQL con esquema completo (SQL Server requiere separar esquema)
            schema_parts = schema.split(".")
            if len(schema_parts) == 2:
                database, schema_name = schema_parts
                insert_sql = text(
                    f"""
                    INSERT INTO [{database}].[{schema_name}].[{table_name}] 
                    ({columns_str})
                    VALUES ({placeholders})
                """
                )
            else:
                insert_sql = text(
                    f"""
                    INSERT INTO [{schema}].[{table_name}] 
                    ({columns_str})
                    VALUES ({placeholders})
                """
                )

            # Ejecutar inserción en lotes para mejor rendimiento
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                session.execute(insert_sql, batch)
                total_inserted += len(batch)

                if total_inserted % 5000 == 0:  # Log cada 5000 registros
                    self.logger.info(
                        f"Insertados {total_inserted}/{records_count} registros..."
                    )

            session.commit()
            self.logger.info(
                f"Inserción completada: {total_inserted} registros en {schema}.{table_name}"
            )

            return total_inserted

        except Exception as e:
            self.logger.error(f"Error en inserción masiva: {str(e)}")
            session.rollback()
            raise

    def get_data_quality_report(self, df: pl.DataFrame) -> Dict:
        """
        Genera un reporte de calidad de datos

        Args:
            df: DataFrame a analizar

        Returns: Diccionario con métricas de calidad
        """
        try:
            if df.is_empty():
                return {"status": "empty", "total_records": 0}

            report = {
                "total_records": len(df),
                "columns": len(df.columns),
                "null_counts": {},
                "data_types": {},
                "unique_counts": {},
            }

            # Contar valores nulos por columna
            for col in df.columns:
                null_count = df.select(pl.col(col).is_null().sum()).item()
                report["null_counts"][col] = null_count
                report["data_types"][col] = str(df[col].dtype)

                # Contar valores únicos para campos clave
                if col in ["IdPlaza", "IdMedio", "IdCanal", "IdMarca"]:
                    unique_count = df.select(pl.col(col).n_unique()).item()
                    report["unique_counts"][col] = unique_count

            return report

        except Exception as e:
            self.logger.error(f"Error generando reporte de calidad: {str(e)}")
            return {"status": "error", "message": str(e)}
