"""
Spark Consumer — EDA Data Ingest Hub
Pipeline: SQS (mensajes de SNS) → S3 → PySpark Transform → PostgreSQL (JDBC) → Event Store

Arquitectura Event-Driven:
  - Las colas SQS (tv-spark-queue, radio-spark-queue, impresos-spark-queue) reciben mensajes
    de SNS, que a su vez los recibe de las S3 Event Notifications.
  - Cada mensaje tiene un envelope SNS que envuelve la notificación de S3.
  - Al completar la carga, actualiza el Event Store (domain_events) marcando el evento
    DataIngested como PROCESSED.

Formato del mensaje recibido en SQS (envelope SNS sobre S3 notification):
  {
    "Type": "Notification",
    "TopicArn": "arn:aws:sns:...:tv-events",
    "Message": "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"raw-data\"},
                 \"object\":{\"key\":\"rawdata/tv/20260101/data.json\"}}}]}"
  }
"""

import os
import json
import time
import logging
import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkConsumer")

#  Configuración 

AWS_ENDPOINT  = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_KEY       = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
DATABASE_URL  = os.getenv("DATABASE_URL", "postgresql://admin:adminpassword@db:5432/datahub_db")
S3_BUCKET     = os.getenv("S3_BUCKET", "raw-data")
SQS_BASE_URL  = os.getenv("SQS_BASE_URL", "http://localstack:4566/000000000000")

# ID único de esta réplica del consumer (para el Event Store)
CONSUMER_ID = f"spark-consumer-{socket.gethostname()}"

#  SQL Server mxTarifas (dual-write)
SS_HOST     = os.getenv("SQLSERVER_HOST", "")
SS_PORT     = os.getenv("SQLSERVER_PORT", "1433")
SS_USER     = os.getenv("SQLSERVER_USER", "")
SS_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")
SS_DB       = os.getenv("SQLSERVER_DB_TARIFAS", "mxTarifas")
SS_DB_SAC   = os.getenv("SQLSERVER_DB_SAC", "appSAC")

# Colas EDA: suscritas al Event Bus SNS (reciben mensajes vía fan-out)
QUEUES = [
    "tv-spark-queue",
    "radio-spark-queue",
    "impresos-spark-queue",
]

# Parsear DATABASE_URL para JDBC
_db_parsed    = urlparse(DATABASE_URL.replace("postgresql+psycopg://", "postgresql://"))
JDBC_URL      = f"jdbc:postgresql://{_db_parsed.hostname}:{_db_parsed.port or 5432}{_db_parsed.path}"
JDBC_USER     = _db_parsed.username or "admin"
JDBC_PASSWORD = _db_parsed.password or "adminpassword"

#  SparkSession 

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FintechEDAConsumer")
        .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

#  Columnas de tablas PostgreSQL 

TV_TABLE_COLS = [
    "Fecha", "IdPlaza", "Plaza", "IdLocalidad", "Localidad", "IdMedio", "Medio",
    "IdTipoMedio", "TipoMedio", "IdCanal", "Canal", "HInicio", "HFinal",
    "IdMarca", "Marca", "IdSubmarca", "Submarca", "IdProducto", "Producto",
    "IdVersion", "Version", "DReal", "DTeorica", "IdTipoSpot", "TipoSpot",
    "IdSubTipoSpot", "SubTipoSpot", "IdTipoCorte", "TipoCorte", "NoCorte", "PEnCorte",
    "IdCampania", "Campania", "IdCorporativo", "Corporativo", "IdAnunciante", "Anunciante",
    "IdAgenciaP", "AgenciaP", "IdCentralMedios", "CentralMedios", "IdIndustria", "Industria",
    "IdMercado", "Mercado", "IdSegmento", "Segmento", "IdDeteccion", "Tarifa", "Testigo",
    "IdPrograma", "Programa", "Subtitulo", "IdGenero", "Genero", "FHoraria",
    "GComercial", "GEstacion", "Origen", "VerCodigo",
]

RADIO_TABLE_COLS = TV_TABLE_COLS

IMPRESOS_TABLE_COLS = [
    "Fecha", "IdMedio", "Medio", "IdFuente", "Fuente", "Autor", "Seccion", "Pagina",
    "Anunciante", "Marca", "Submarca", "Producto",
    "Sector", "Subsector", "Categoria",
    "Tiraje", "Costo", "Dimension", "Testigo", "TextoNota",
]

#  Cliente SQS 

def get_sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )

#  Parseo de mensajes EDA (envelope SNS → S3 notification) 

def parse_s3_notification(raw_body: str):
    """
    Deserializa el mensaje SQS que viene del fan-out SNS.
    El envelope SNS contiene la S3 Event Notification como string JSON en el campo "Message".

    Retorna (s3_key, tipo, fecha) o (None, None, None) si no es un evento de creación.
    """
    try:
        outer = json.loads(raw_body)

        # Caso 1: mensaje viene de SNS (envelope con "Type": "Notification")
        if outer.get("Type") == "Notification":
            inner = json.loads(outer["Message"])
        else:
            # Caso 2: mensaje directo (compatibilidad con formato legacy o replay manual)
            inner = outer

        # Formato S3 Event Notification
        records = inner.get("Records", [])
        if not records:
            return None, None, None

        record = records[0]
        s3_info = record.get("s3", {})
        event_name = record.get("eventName", "")

        # Solo procesar eventos de creación de objetos
        if not event_name.startswith("ObjectCreated"):
            logger.info(f"Ignorando evento S3: {event_name}")
            return None, None, None

        s3_key = s3_info.get("object", {}).get("key", "")
        if not s3_key:
            return None, None, None

        # Inferir tipo y fecha desde el path: rawdata/{tipo}/{fecha}/data.json
        parts = s3_key.split("/")
        if len(parts) < 4 or parts[0] != "rawdata":
            logger.warning(f"Path S3 no reconocido: {s3_key}")
            return None, None, None

        tipo  = parts[1]   # tv | radio | impresos
        fecha = parts[2]   # YYYYMMDD

        return s3_key, tipo, fecha

    except Exception as e:
        logger.error(f"Error parseando mensaje S3 notification: {e} | body={raw_body[:300]}")
        return None, None, None

#  Transform helpers 

def cast_fecha(df):
    return df.withColumn(
        "Fecha",
        F.when(
            F.col("Fecha").contains("T"),
            F.to_date(F.col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss"),
        ).otherwise(
            F.to_date(F.col("Fecha"), "yyyyMMdd")
        ),
    )

def _normalize_str(value):
    """Quita acentos, caracteres raros y convierte a MAYÚSCULAS."""
    if value is None:
        return None
    import unicodedata
    import re
    nfkd = unicodedata.normalize("NFKD", str(value))
    without_accents = "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")
    upper = without_accents.upper()
    return re.sub(r"[^\x20-\x7E\n]", "", upper)


# UDF de Spark para normalización de strings
_normalize_udf = F.udf(_normalize_str, StringType())


def clean_strings(df, cols, max_len=255):
    """Recorta, normaliza (sin acentos, MAYÚSCULAS) y limpia columnas de texto. Asigna 'N.A.' a nulos/vacíos."""
    for col in cols:
        if col in df.columns:
            trimmed = F.trim(F.substring(F.col(col).cast("string"), 1, max_len))
            normalized = _normalize_udf(trimmed)
            df = df.withColumn(
                col,
                F.when(normalized.isNull() | (F.trim(normalized) == ""), F.lit("N.A.")).otherwise(normalized),
            )
    return df

def cast_numerics(df, int_cols, long_cols, double_cols):
    """Castea columnas numéricas y asigna 0 a valores nulos."""
    for col in int_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col).cast(IntegerType()), F.lit(0)))
    for col in long_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col).cast(LongType()), F.lit(0).cast(LongType())))
    for col in double_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col).cast(DoubleType()), F.lit(0.0)))
    return df

TV_RADIO_STRING_COLS = [
    "Plaza", "Localidad", "Medio", "TipoMedio", "Canal", "Marca", "Submarca",
    "Producto", "TipoSpot", "SubTipoSpot", "TipoCorte", "Campania", "Corporativo",
    "Anunciante", "AgenciaP", "CentralMedios", "Industria", "Mercado", "Segmento",
    "Programa", "Genero", "FHoraria", "GComercial", "GEstacion", "Origen",
    "VerCodigo", "HInicio", "HFinal", "PEnCorte", "Cobertura", "SiglasEst",
    "EstCanal", "CodigoAcam",
]
TV_RADIO_INT_COLS = [
    "IdPlaza", "IdLocalidad", "IdMedio", "IdTipoMedio", "IdCanal", "IdMarca",
    "IdSubmarca", "IdProducto", "IdTipoSpot", "IdSubTipoSpot", "IdTipoCorte",
    "NoCorte", "IdCampania", "IdCorporativo", "IdAnunciante", "IdAgenciaP",
    "IdCentralMedios", "IdIndustria", "IdMercado", "IdSegmento", "IdPrograma",
    "IdGenero", "DReal", "DTeorica",
]
TV_RADIO_LONG_COLS   = ["IdVersion", "IdDeteccion"]
TV_RADIO_DOUBLE_COLS = ["Tarifa"]

IMPRESOS_STRING_COLS = [
    "Medio", "Fuente", "Autor", "Seccion", "Pagina",
    "Anunciante", "Marca", "Submarca", "Producto",
    "Sector", "Subsector", "Categoria", "Testigo", "TextoNota",
]
IMPRESOS_INT_COLS    = ["IdMedio", "IdFuente"]
IMPRESOS_LONG_COLS   = ["Tiraje"]
IMPRESOS_DOUBLE_COLS = ["Costo", "Dimension"]

def transform_tv_radio(df):
    df = cast_fecha(df)
    df = df.filter(F.col("Fecha").isNotNull() & F.col("IdDeteccion").isNotNull())
    df = clean_strings(df, TV_RADIO_STRING_COLS)
    df = cast_numerics(df, TV_RADIO_INT_COLS, TV_RADIO_LONG_COLS, TV_RADIO_DOUBLE_COLS)
    if "Subtitulo" not in df.columns:
        df = df.withColumn("Subtitulo", F.lit("N.A."))
    else:
        df = df.withColumn(
            "Subtitulo",
            F.when(F.col("Subtitulo").isNull() | (F.trim(F.col("Subtitulo")) == ""), F.lit("N.A."))
            .otherwise(F.trim(F.col("Subtitulo"))),
        )
    return df

def transform_impresos(df):
    df = cast_fecha(df)
    df = df.filter(F.col("Fecha").isNotNull() & F.col("IdMedio").isNotNull())
    df = clean_strings(df, IMPRESOS_STRING_COLS, max_len=128)
    df = cast_numerics(df, IMPRESOS_INT_COLS, IMPRESOS_LONG_COLS, IMPRESOS_DOUBLE_COLS)
    return df

#  PostgreSQL helpers (psycopg2 directo) 

def _pg_conn():
    """
    Crea conexión directa a PostgreSQL usando psycopg2. 
    Utilizada para operaciones CRUD directas en la base de datos.
    """
    parsed = urlparse(DATABASE_URL.replace("postgresql+psycopg://", "postgresql://"))
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )

def log_start(source: str, instruction: str, total_rows: int) -> int:
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO public.auditsa_api_log
               (source, date_time, instruction, total_rows, total_records, status)
               VALUES (%s, %s, %s, %s, 0, 'RECORDING') RETURNING id""",
            (source, datetime.now(timezone.utc).replace(tzinfo=None), instruction, total_rows),
        )
        log_id = cur.fetchone()[0]
        conn.commit()
    return log_id

def log_complete(log_id: int, total_records: int) -> None:
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE public.auditsa_api_log SET total_records=%s, status='SUCCESS' WHERE id=%s",
            (total_records, log_id),
        )
        conn.commit()

def event_store_mark_processed(aggregate_type: str, aggregate_id: str) -> None:
    """
    Marca el evento DataIngested correspondiente como PROCESSED en el Event Store.
    Dual-write: actualiza PostgreSQL (domain_events) y SQL Server (appSAC.app.domain_events_SAC).
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # PostgreSQL
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE public.domain_events
                   SET status='PROCESSED',
                       processed_at=%s,
                       consumer_id=%s
                   WHERE aggregate_type=%s
                     AND aggregate_id=%s
                     AND event_type='DataIngested'
                     AND status='PENDING'""",
                (now, CONSUMER_ID, aggregate_type, aggregate_id),
            )
            updated = cur.rowcount
            conn.commit()
        if updated:
            logger.info(f"EventStore PG: DataIngested PROCESSED | {aggregate_type}/{aggregate_id} | consumer={CONSUMER_ID}")
        else:
            logger.warning(f"EventStore PG: no se encontró evento PENDING para {aggregate_type}/{aggregate_id}")
    except Exception as e:
        logger.error(f"Error actualizando Event Store PostgreSQL: {e}")

    # SQL Server appSAC.app.domain_events_SAC (dual-write, no-fatal)
    if not SS_HOST:
        return
    try:
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        props = sc._jvm.java.util.Properties()
        props.setProperty("user", SS_USER)
        props.setProperty("password", SS_PASSWORD)
        conn = sc._jvm.java.sql.DriverManager.getConnection(SS_SAC_JDBC_URL, props)
        ps = conn.prepareStatement(
            "UPDATE [app].[domain_events_SAC] "
            "SET [status]=?, [processed_at]=?, [consumer_id]=? "
            "WHERE [aggregate_type]=? AND [aggregate_id]=? "
            "AND [event_type]='DataIngested' AND [status]='PENDING'"
        )
        ps.setString(1, "PROCESSED")
        ps.setString(2, now.strftime("%Y-%m-%d %H:%M:%S"))
        ps.setString(3, CONSUMER_ID)
        ps.setString(4, aggregate_type)
        ps.setString(5, aggregate_id)
        rows = ps.executeUpdate()
        conn.commit()
        ps.close()
        conn.close()
        if rows:
            logger.info(f"EventStore SS SAC: DataIngested PROCESSED | {aggregate_type}/{aggregate_id}")
        else:
            logger.warning(f"EventStore SS SAC: no se encontró evento PENDING para {aggregate_type}/{aggregate_id}")
    except Exception as e:
        logger.warning(f"SQL Server SAC event_store_mark_processed failed (non-fatal): {e}")

#  Carga a PostgreSQL via JDBC (idempotente: delete-before-insert por fecha) 

def delete_existing_by_fecha(table: str, fecha: str) -> int:
    """
    Elimina registros existentes para una fecha antes de insertar.
    Garantiza idempotencia: re-procesar la misma fecha no genera duplicados.
    """
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM public.{table} WHERE "Fecha" = %s',
                (fecha,),
            )
            deleted = cur.rowcount
            conn.commit()
        if deleted:
            logger.info(f"IDEMPOTENCY: {deleted} registros previos eliminados de {table} para fecha={fecha}")
        return deleted
    except Exception as e:
        logger.error(f"Error eliminando registros previos de {table} fecha={fecha}: {e}")
        return 0


def load_to_postgres(df, table: str, table_cols: list, fecha: str = None) -> int:
    existing = [c for c in table_cols if c in df.columns]
    df_filtered = df.select(existing)
    count = df_filtered.count()

    # Idempotencia: eliminar datos previos de la misma fecha antes de insertar
    if fecha:
        delete_existing_by_fecha(table, fecha)

    (
        df_filtered.write
        .mode("append")
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"public.{table}")
        .option("driver", "org.postgresql.Driver")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("batchsize", 1000)
        .save()
    )
    logger.info(f"LOAD: {count} registros → public.{table}")
    return count

#  SQL Server dual-write (Spark JDBC — sin pymssql, mismo mecanismo que PostgreSQL)

_SS_TABLE_MAP = {
    "tv":       ("auditsa_api_tv",       TV_TABLE_COLS),
    "radio":    ("auditsa_api_radio",    RADIO_TABLE_COLS),
    "impresos": ("auditsa_api_impresos", IMPRESOS_TABLE_COLS),
}

# JDBC URL para SQL Server (encrypt=false evita problemas de certificado en LAN)
SS_JDBC_URL = (
    f"jdbc:sqlserver://{SS_HOST}:{SS_PORT};"
    f"databaseName={SS_DB};encrypt=false;trustServerCertificate=true;"
    f"loginTimeout=15;"
) if SS_HOST else ""

SS_SAC_JDBC_URL = (
    f"jdbc:sqlserver://{SS_HOST}:{SS_PORT};"
    f"databaseName={SS_DB_SAC};encrypt=false;trustServerCertificate=true;"
    f"loginTimeout=15;"
) if SS_HOST else ""


def _delete_sqlserver_fecha(table_name: str, fecha: str) -> None:
    """Elimina registros de la fecha en SQL Server vía psycopg2-style usando JDBC driver."""
    if not SS_HOST:
        return
    try:
        # Usamos el driver JDBC ya cargado por Spark vía sc._jvm
        from pyspark import SparkContext
        sc = SparkContext._active_spark_context
        props = sc._jvm.java.util.Properties()
        props.setProperty("user", SS_USER)
        props.setProperty("password", SS_PASSWORD)
        conn = sc._jvm.java.sql.DriverManager.getConnection(SS_JDBC_URL, props)
        stmt = conn.createStatement()
        delete_sql = f"DELETE FROM [dbo].[{table_name}] WHERE [Fecha] = '{fecha}'"
        stmt.executeUpdate(delete_sql)
        conn.commit()
        stmt.close()
        conn.close()
    except Exception as e:
        logger.warning(f"SQL Server DELETE fecha={fecha} en {table_name}: {e}")


def write_to_sqlserver(df, tipo: str, fecha: str) -> int:
    """
    Dual-write a SQL Server mxTarifas usando Spark JDBC.
    Mismo mecanismo que load_to_postgres — sin pymssql ni pandas.
    No-fatal: si falla loguea el error sin interrumpir el pipeline.
    """
    if not SS_HOST:
        logger.warning("SQLSERVER_HOST no configurado — omitiendo dual-write SQL Server")
        return 0

    table_name, all_cols = _SS_TABLE_MAP[tipo]
    columns = [c for c in all_cols if c in df.columns]
    df_ss = df.select(columns)
    count = df_ss.count()

    try:
        # Idempotencia: eliminar fecha existente antes de insertar
        _delete_sqlserver_fecha(table_name, fecha)

        (
            df_ss.write
            .mode("append")
            .format("jdbc")
            .option("url", SS_JDBC_URL)
            .option("dbtable", f"dbo.{table_name}")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("user", SS_USER)
            .option("password", SS_PASSWORD)
            .option("batchsize", 1000)
            .save()
        )

        logger.info(f"SQL Server {SS_DB}.dbo.{table_name} — {count} registros insertados para fecha={fecha}")
        return count

    except Exception as e:
        logger.error(f"SQL Server dual-write FAILED ({tipo} fecha={fecha}): {e}", exc_info=True)
        return 0


#  Procesamiento de mensajes

def process_message(spark: SparkSession, raw_body: str):
    """
    Procesa un mensaje de la cola SQS.
    El mensaje puede ser:
      - S3 Event Notification envuelto en envelope SNS (flujo EDA normal)
      - Mensaje directo de replay desde el Event Store
    """
    s3_key, tipo, fecha = parse_s3_notification(raw_body)

    if not s3_key:
        logger.warning(f"Mensaje no procesable (no es una S3 notification válida)")
        return

    # Ignorar archivos .keep (placeholders de estructura)
    if s3_key.endswith(".keep"):
        logger.info(f"Ignorando placeholder: {s3_key}")
        return

    s3_path = f"s3a://{S3_BUCKET}/{s3_key}"
    logger.info(f"SPARK READ: tipo={tipo} | fecha={fecha} | path={s3_path}")

    df = spark.read.option("multiLine", True).json(s3_path)

    if df.rdd.isEmpty():
        logger.warning(f"Sin datos en {s3_path}")
        return

    logger.info(f"SPARK: {df.count()} registros leídos — {len(df.columns)} columnas")

    if tipo == "tv":
        df_clean = transform_tv_radio(df)
        table = "auditsa_api_tv"
        log_id = log_start(f"public.{table}", "INSERT", df_clean.count())
        count = load_to_postgres(df_clean, table, TV_TABLE_COLS, fecha=fecha)
        log_complete(log_id, count)
        write_to_sqlserver(df_clean, "tv", fecha)

    elif tipo == "radio":
        df_clean = transform_tv_radio(df)
        table = "auditsa_api_radio"
        log_id = log_start(f"public.{table}", "INSERT", df_clean.count())
        count = load_to_postgres(df_clean, table, RADIO_TABLE_COLS, fecha=fecha)
        log_complete(log_id, count)
        write_to_sqlserver(df_clean, "radio", fecha)

    elif tipo == "impresos":
        df_clean = transform_impresos(df)
        table = "auditsa_api_impresos"
        log_id = log_start(f"public.{table}", "INSERT", df_clean.count())
        count = load_to_postgres(df_clean, table, IMPRESOS_TABLE_COLS, fecha=fecha)
        log_complete(log_id, count)
        write_to_sqlserver(df_clean, "impresos", fecha)

    else:
        logger.warning(f"Tipo desconocido: {tipo}")
        return

    # Actualizar Event Store: DataIngested → PROCESSED
    event_store_mark_processed(aggregate_type=tipo, aggregate_id=fecha)

#  Loop principal 

def main():
    spark = create_spark_session()
    sqs   = get_sqs_client()

    logger.info(f"Spark EDA Consumer iniciado | consumer_id={CONSUMER_ID}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Colas EDA: {QUEUES}")
    logger.info("Flujo: SQS (SNS fan-out de S3 Event Notifications) → Spark → PostgreSQL → Event Store")

    while True:
        for queue_name in QUEUES:
            queue_url = f"{SQS_BASE_URL}/{queue_name}"
            try:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                )
                messages = response.get("Messages", [])

                for msg in messages:
                    try:
                        process_message(spark, msg["Body"])
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                    except Exception as e:
                        logger.error(f"Error procesando mensaje de {queue_name}: {e}", exc_info=True)
                        logger.error(f"Body: {msg.get('Body', 'N/A')[:500]}")

            except Exception as e:
                logger.error(f"Error conectando a {queue_name}: {e}")
                time.sleep(5)

        time.sleep(1)


if __name__ == "__main__":
    main()
