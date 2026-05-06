# Services — Capa de Servicios

Contiene toda la logica de negocio del pipeline EDA: comunicacion con la API de Auditsa, pipelines ETL por tipo de medio, integracion con AWS (S3, SQS, SNS), escritura dual a SQL Server y servicios de soporte (logging, Event Store, DLQ, cache, auditoria).

---

## Archivos

### API / Extraccion

| Archivo | Clase | Descripcion |
|---|---|---|
| `auditsa_api_services.py` | `AuditsaApiService` | Cliente HTTP de la API de Auditsa. Expone `get_tv_data`, `get_radio_data`, `get_impresos_data`, `get_media_master_library_data` y `get_media_master_totals_data`. Lee credenciales desde variables de entorno (`AUDITSA_URL`, `AUDITSA_TOKEN`, `AUDITSA_CLID`). Timeout de 300 s por request. |
| `auditsa_data_processor.py` | `AuditsaDataProcessor` | Procesador generico con Pandas. Valida schema, castea tipos, limpia strings y genera reportes de calidad de datos. Insercion masiva por lotes de 1,000 registros. |

### Pipelines ETL

Cada servicio implementa el mismo patron **Extract → Transform → Load** con metodos `run_etl_pipeline(fecha)` y `run_etl_pipeline_range(fecha_inicio, fecha_fin)`.

| Archivo | Clase | Medio | Endpoint Auditsa | Tabla destino |
|---|---|---|---|---|
| `auditsa_tv_etl_service.py` | `AuditsaTvEtlService` | Television (dedicado) | `/GetHits` | `public.auditsa_api_tv` |
| `auditsa_radio_etl_service.py` | `AuditsaRadioEtlService` | Radio (automatico, IDs 1 y 2) | `/GetHitsAutomatico` | `public.auditsa_api_radio` |
| `auditsa_impresos_etl_service.py` | `AuditsaImpresosEtlService` | Medios impresos (IDs 1, 2, 4) | `/GetPrintedHits` | `public.auditsa_api_impresos` |
| `auditsa_media_master_library_etl_service.py` | `AuditsaMediaMasterLibraryEtlService` | Catalogo de versiones/spots | `/GetMediaMasterLibrary` | `public.auditsa_api_media_master_library` |
| `auditsa_media_master_totals_etl_service.py` | `AuditsaMediaMasterTotalsEtlService` | Totales agregados por periodo | `/GetMediaMasterTotals` | `public.auditsa_api_media_master_totals` |

Cada servicio expone tambien:
- `get_etl_status(fecha)` / `get_etl_status_range(fecha_inicio, fecha_fin)` — estadisticas de registros en BD.
- `delete_*_data(fecha)` / `delete_*_data_range(fecha_inicio, fecha_fin)` — eliminacion por fecha con `TO_DATE`.

La transformacion usa **Pandas**: conversion robusta de fechas (multiples formatos), cast de tipos numericos con `pd.to_numeric(errors="coerce")`, truncado de strings a 255 caracteres y limpieza de nulos → `"N.A."`.

### Infraestructura AWS

| Archivo | Clase | Descripcion |
|---|---|---|
| `s3_service.py` | `S3Service` | Sube (`upload_json`), descarga (`download_json`) y elimina por prefijo (`delete_prefix`) objetos JSON en S3. Usa boto3; compatible con LocalStack (`AWS_ENDPOINT_URL`). Bucket: `S3_BUCKET` (default `raw-data`). |
| `sqs_service.py` | `SQSService` | Envia (`send_message`), recibe (`receive_messages`, long-polling 5 s) y elimina (`delete_message`) mensajes de colas SQS. Cachea URLs de cola para reducir llamadas a la API. |
| `sns_service.py` | `SNSService` | Publica eventos de dominio al Event Bus central (SNS). El ARN base se configura con `SNS_BASE_ARN`. Helper `topic_name_for(tipo)` devuelve el topic por tipo de medio. |

### Escritura Dual (SQL Server)

| Archivo | Clase / Modulo | Descripcion |
|---|---|---|
| `sqlserver_write_service.py` | Modulo (`upsert_to_sqlserver`) | Escritura dual a SQL Server mxTarifas. Se llama despues del commit a PostgreSQL en cada ETL service. Si SQL Server falla, el error es **no-fatal**: el commit a Postgres ya esta hecho (graceful degradation). Soporta todos los tipos: `tv`, `radio`, `impresos`, `media_master_library`, `media_master_totals`. Operacion: `DELETE WHERE Fecha = ? → bulk INSERT` en lotes de 1,000 registros. |

**Mapeo de tablas en mxTarifas:**

| Tipo | Tabla SQL Server |
|---|---|
| `tv` | `dbo.auditsa_api_tv` |
| `radio` | `dbo.auditsa_api_radio` |
| `impresos` | `dbo.auditsa_api_impresos` |
| `media_master_library` | `dbo.auditsa_api_media_master_library` |
| `media_master_totals` | `dbo.auditsa_api_media_master_totals` |

### Soporte y Observabilidad

| Archivo | Clase / Modulo | Descripcion |
|---|---|---|
| `log_service.py` | `LogService` | Ciclo de vida de un registro de log ETL en `auditsa_api_log`. `start()` crea un registro `RECORDING`; `complete()` lo cierra con `SUCCESS` u otro estado. |
| `event_store_service.py` | `EventStoreService` | Persistencia de eventos de dominio inmutables en `domain_events`. Estados: `PENDING → PROCESSED / FAILED`. Metodos: `publish`, `mark_processed`, `mark_failed`, `get_events`. |
| `dlq_service.py` | `DLQService` | Gestion de Dead Letter Queues SQS por tipo de medio. Expone `get_stats`, `list_messages`, `replay_all` (mueve mensajes del DLQ de vuelta a la cola Spark y resetea domain_events `FAILED → PENDING`) y `purge`. |
| `stats_cache_service.py` | Modulo (`get` / `set` / `invalidate`) | Cache en memoria con TTL (defecto 10 min) para stats de tablas grandes. Evita queries de ~40 s sobre volumenes de 20 M+ filas en cada peticion. |
| `session_log_service.py` | Modulo (`build_sql` / `log_action`) | Auditoria de queries ejecutadas por usuarios. `build_sql` construye una representacion legible del SELECT o EXPORT ejecutado. `log_action` escribe la entrada en PostgreSQL y SQL Server appSAC como BackgroundTask (no bloquea la respuesta HTTP). |

### Control de Acceso

| Archivo | Clase / Modulo | Descripcion |
|---|---|---|
| `account_sectors_service.py` | Modulo (`get_allowed_media`) | Consulta en SQL Server appSAC los tipos de medio permitidos para una cuenta (`id_account`). Retorna lista de strings como `["tv", "radio"]`. Se llama al generar el token JWT para incluir `allowed_media` en el payload. |
| `string_normalizer.py` | Modulo | Normalizacion de strings: eliminacion de acentos, conversion a minusculas, limpieza de caracteres especiales. Util para comparaciones case-insensitive. |

---

## Dependencias entre servicios

```
Routers (tv / radio / impresos)
  └─► ETL Service (Extract → Transform → Load)
        ├─► AuditsaApiService       — extrae datos crudos de la API
        ├─► S3Service               — persiste raw JSON en Data Lake
        │     └─► [S3 dispara SNS → SQS → Spark Consumer]
        ├─► EventStoreService       — registra evento de dominio inmutable
        ├─► SqlServerWriteService   — dual-write a SQL Server mxTarifas
        ├─► LogService              — registra ejecucion en auditsa_api_log
        └─► StatsCache.invalidate   — invalida cache si corresponde

Routers (records / export)
  └─► session_log_service.log_action (BackgroundTask)
        ├─► PostgreSQL auditsa_api_log
        └─► SQL Server appSAC (graceful degradation)

DLQ Router
  └─► DLQService
        ├─► SQSService (DLQ → cola Spark)
        └─► EventStoreService (reset FAILED → PENDING)

Auth Router /token
  └─► account_sectors_service.get_allowed_media (SQL Server appSAC)
        └─► Incluye allowed_media en payload JWT

Auth Router /register
  └─► PostgreSQL (commit obligatorio)
  └─► SQL Server appSAC (graceful degradation)
```

---

## Variables de entorno requeridas

| Variable | Uso |
|---|---|
| `AUDITSA_URL` | Base URL de la API de Auditsa |
| `AUDITSA_TOKEN` | Token de autenticacion |
| `AUDITSA_CLID` | Client ID |
| `STR_START` / `STR_END` | Sufijos de hora para `perIni` / `perFin` (TV y Radio) |
| `AWS_ENDPOINT_URL` | Endpoint de LocalStack (desarrollo) o AWS (produccion) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credenciales AWS |
| `AWS_REGION` | Region AWS (default `us-east-1`) |
| `S3_BUCKET` | Nombre del bucket S3 (default `raw-data`) |
| `SNS_BASE_ARN` | ARN base de SNS (default `arn:aws:sns:us-east-1:000000000000`) |
| `SQLSERVER_HOST` | Host SQL Server para dual-write |
| `SQLSERVER_DB_TARIFAS` | Base de datos mxTarifas en SQL Server |
