# spark-jobs — Spark Consumer EDA

Contiene el **Spark Consumer**: proceso de larga duración que escucha las colas SQS del pipeline EDA, descarga los datos crudos desde S3, aplica transformaciones con PySpark y los carga en PostgreSQL vía JDBC. Al finalizar, actualiza el Event Store marcando el evento como `PROCESSED`.

---

## Archivos

| Archivo | Descripción |
|---|---|
| `consumer.py` | Spark Consumer — loop infinito que consume mensajes de las 3 colas SQS |

---

## Flujo de procesamiento

```
SQS Queue ({tipo}-spark-queue)
  └─► Mensaje: envelope SNS → S3 Event Notification
        │
        ▼
  parse_s3_notification()
    Extrae: s3_key, tipo (tv|radio|impresos), fecha (YYYYMMDD)
        │
        ▼
  spark.read.json(s3a://raw-data/{s3_key})
        │
        ▼
  transform_{tipo}(df)           ← PySpark: cast fechas, limpiar strings, cast numéricos
        │
        ▼
  load_to_postgres(df, table)    ← JDBC append → public.auditsa_api_{tipo}
        │
        ▼
  log_complete()                 ← auditsa_api_log: RECORDING → SUCCESS
        │
        ▼
  event_store_mark_processed()   ← domain_events: PENDING → PROCESSED
        │
        ▼
  sqs.delete_message()           ← elimina el mensaje de la cola
```

---

## Descripción de componentes

### Loop principal (`main`)

Corre indefinidamente iterando sobre las 3 colas en orden:

```
tv-spark-queue → radio-spark-queue → impresos-spark-queue → sleep(1s) → repetir
```

Cada cola usa **long-polling de 5 s** y recibe hasta 10 mensajes por llamada. Si un mensaje falla su procesamiento, se loguea el error pero **no se elimina** de la cola — SQS lo reintentará hasta `maxReceiveCount=3`, tras lo cual pasa al DLQ correspondiente.

### Parseo de mensajes (`parse_s3_notification`)

El formato que llega a SQS es un **envelope SNS** que envuelve la S3 Event Notification:

```json
{
  "Type": "Notification",
  "TopicArn": "arn:aws:sns:...:tv-events",
  "Message": "{\"Records\":[{\"s3\":{\"object\":{\"key\":\"rawdata/tv/20260101/data.json\"}}}]}"
}
```

El parser también acepta mensajes directos (sin envelope) para compatibilidad con replay manual desde `/api/events/{id}/replay`.

El tipo de medio y la fecha se infieren del path S3: `rawdata/{tipo}/{fecha}/data.json`.

### Transformaciones PySpark

#### TV y Radio (`transform_tv_radio`)

| Paso | Operación |
|---|---|
| Fecha | `cast_fecha`: detecta formato ISO (`T`) o `yyyyMMdd`, convierte a `DateType` |
| Filtrado | Descarta registros sin `Fecha` o sin `IdDeteccion` |
| Strings | `clean_strings`: trim + truncar a 255 chars + reemplazar nulos/vacíos con `"N.A."` |
| Enteros | `cast_numerics` → `IntegerType`, nulos → `0` |
| Long | `IdVersion`, `IdDeteccion` → `LongType` |
| Double | `Tarifa` → `DoubleType` |

#### Impresos (`transform_impresos`)

Mismo patrón que TV/Radio con diferencias:
- Filtrado por `IdMedio` en lugar de `IdDeteccion`
- Strings truncados a 128 chars
- Columnas numéricas: `Costo`, `Dimension` → `DoubleType`; `Tiraje` → `LongType`

### Carga a PostgreSQL (`load_to_postgres`)

Usa el conector JDBC de PostgreSQL con `mode("append")` y `batchsize=1000`. Solo se escriben las columnas que existen tanto en el DataFrame como en el schema de la tabla destino.

| Tipo | Tabla destino |
|---|---|
| `tv` | `public.auditsa_api_tv` |
| `radio` | `public.auditsa_api_radio` |
| `impresos` | `public.auditsa_api_impresos` |

### Event Store (`event_store_mark_processed`)

Al completar la carga, actualiza `public.domain_events` buscando el evento `DataIngested` en estado `PENDING` que corresponda a `(aggregate_type, aggregate_id)` y lo marca como `PROCESSED`, registrando el `consumer_id` (hostname del contenedor) y el `processed_at`.

---

## Configuración

### Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `AWS_ENDPOINT_URL` | `http://localstack:4566` | Endpoint LocalStack / AWS real |
| `AWS_ACCESS_KEY_ID` | `test` | Access key |
| `AWS_SECRET_ACCESS_KEY` | `test` | Secret key |
| `AWS_REGION` | `us-east-1` | Región AWS |
| `DATABASE_URL` | `postgresql://admin:adminpassword@db:5432/datahub_db` | Conexión PostgreSQL |
| `S3_BUCKET` | `raw-data` | Bucket del Data Lake |
| `SQS_BASE_URL` | `http://localstack:4566/000000000000` | Base URL de las colas SQS |

### Dependencias Python (instaladas en runtime via `spark-submit`)

```
boto3, sqlalchemy, psycopg2-binary, prometheus-client, jsonschema
```

### Paquetes Spark (`--packages` en docker-compose)

| Paquete | Propósito |
|---|---|
| `hadoop-aws:3.3.4` | Conector S3A para leer desde S3/LocalStack |
| `postgresql:42.6.0` | Driver JDBC PostgreSQL |
| `spark-excel_2.12` | (reservado para futuros jobs Excel) |

### Configuración SparkSession

```
spark.sql.caseSensitive = true          ← las columnas de Auditsa usan PascalCase
spark.hadoop.fs.s3a.path.style.access = true   ← requerido para LocalStack
spark.hadoop.fs.s3a.connection.ssl.enabled = false
```

---

## Despliegue

En `docker-compose.yml` el consumer corre con **2 réplicas** (`deploy.replicas: 2`). Docker Compose asigna nombres automáticamente (`spark-processor-1`, `spark-processor-2`). El `CONSUMER_ID` se genera con el hostname del contenedor para identificar qué réplica procesó cada evento en el Event Store.

```yaml
spark-processor:
  image: apache/spark:3.5.0
  deploy:
    replicas: 2
  volumes:
    - ./spark-jobs:/app/jobs
```

Para agregar réplicas adicionales, modificar `replicas` en `docker-compose.yml` y reiniciar el servicio.

---

## Manejo de fallos

| Escenario | Comportamiento |
|---|---|
| Error en `process_message` | Se loguea, el mensaje **no se elimina** → SQS lo reintentará |
| 3 intentos fallidos | SQS mueve el mensaje al DLQ (`{tipo}-spark-dlq`) |
| Replay desde DLQ | `POST /api/dlq/{tipo}/replay` mueve mensajes de vuelta al spark-queue |
| Replay desde Event Store | `POST /api/events/{event_id}/replay` publica de nuevo al topic SNS |
| Error de conexión a SQS | `sleep(5s)` y reintento en el siguiente ciclo del loop |
