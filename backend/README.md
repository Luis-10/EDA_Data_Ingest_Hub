# Backend — EDA Data Ingest Hub

Aplicacion **FastAPI** que implementa la capa de ingesta de datos publicitarios (TV, Radio, Impresos) desde la API de Auditsa hacia un Data Lake en S3 y un Data Warehouse en PostgreSQL y SQL Server, usando una **Arquitectura Event-Driven (EDA)**.

---

## Arquitectura general

```
Cliente / Scheduler
        │
        ▼
  FastAPI (puerto 8000)
        │
  ┌─────▼───────────────────────────────────────────────────┐
  │  POST /api/{tipo}/etl                                   │
  │    1. Extract      ← AuditsaApiService                  │
  │    2. S3 Upload    ← S3Service  (rawdata/{tipo}/{fecha})│
  │         └─► S3 Event Notification → SNS Topic           │
  │                           └─► {tipo}-spark-queue        │
  │    3. EventStore   ← EventStoreService (DataIngested)   │
  │    4. Dual-write   ← SqlServerWriteService (mxTarifas)  │
  │    5. Log          ← LogService                         │
  │    6. Retorna 202 Accepted + event_id                   │
  └─────────────────────────────────────────────────────────┘
        │
        ▼
  Spark Consumer (2 replicas)
    Lee SQS → descarga S3 → transforma → escribe PostgreSQL
        │
        ▼
  PostgreSQL (public schema)
    auditsa_api_tv | auditsa_api_radio | auditsa_api_impresos
    auditsa_api_log | domain_events | catalogue_users | catalogue_roles

  SQL Server mxTarifas (dual-write, graceful degradation)
    dbo.auditsa_api_tv | dbo.auditsa_api_radio | dbo.auditsa_api_impresos
```

**DELETE** sigue el flujo inverso: elimina objetos S3 → elimina registros en BD → publica `DataDeleted` en SNS → registra en Event Store.

---

## Estructura de directorios

```
backend/
├── main.py               ← Entrypoint FastAPI: startup, routers, indices, prewarm cache
├── Dockerfile            ← Python 3.13-slim con psql, freetds-dev
├── requirements.txt      ← 24+ dependencias Python
├── .env                  ← Variables de entorno locales (no incluir en git)
│
├── config/
│   ├── settings.py       ← Pydantic BaseSettings (40+ variables de entorno)
│   ├── database.py       ← SQLAlchemy engines: PostgreSQL + SQL Server (mxTarifas + appSAC)
│   ├── auth.py           ← JWT + bcrypt (tokens, roles, allowed_media)
│   ├── limiter.py        ← Rate limiting (200 req/min via slowapi)
│   └── schemas.py        ← Schemas Pydantic de requests
│
├── models/               ← Modelos ORM (SQLAlchemy)
│   ├── domain_events.py                      ← Event Store: tabla domain_events
│   ├── api_auditsa_tv.py                     ← Spots TV (~53 columnas)
│   ├── api_auditsa_radio.py                  ← Spots Radio (~53 columnas)
│   ├── api_auditsa_impresos.py               ← Avisos impresos (~19 columnas)
│   ├── api_auditsa_logs.py                   ← Logs de ejecucion ETL
│   ├── api_auditsa_media_master_library.py   ← Catalogo de versiones/spots
│   ├── api_auditsa_media_master_totals.py    ← Totales agregados por periodo
│   ├── catalogues_user_role.py               ← Usuarios y roles (PostgreSQL)
│   ├── catalogues_accounts.py                ← Cuentas (PostgreSQL)
│   ├── sqlserver_auditsa.py                  ← Modelos SQL Server mxTarifas (TV/Radio/Impresos)
│   └── sqlserver_users.py                    ← Modelos SQL Server appSAC (usuarios, cuentas)
│
├── routers/              ← Endpoints FastAPI (15 archivos)
│   ├── auth.py           ← /auth/* — login, registro, cuentas, me
│   ├── tv.py             ← /api/tv — ETL, stats, status, CRUD
│   ├── radio.py          ← /api/radio — ETL, stats, status, CRUD
│   ├── impresos.py       ← /api/impresos — ETL, stats, status, CRUD
│   ├── events.py         ← /api/events — Event Store queries, replay
│   ├── dlq.py            ← /api/dlq — Dead Letter Queue management
│   ├── autocomplete.py   ← /api/autocomplete — filtros en cascada cross-tabla
│   ├── records.py        ← /api/records/{tipo} — registros paginados
│   ├── export.py         ← /api/export/{tipo} — exportacion Excel (max 1M filas)
│   ├── stats.py          ← /api/stats/summary — stats batch de los 3 medios
│   ├── admin.py          ← /admin/* — init SQL Server, cache refresh
│   ├── health.py         ← /health/sqlserver — health check SQL Server externo
│   ├── media_master_library.py  ← /api/media-master-library
│   └── media_master_totals.py   ← /api/media-master-totals
│
├── services/             ← Logica de negocio (18 archivos)
│   ├── auditsa_api_services.py                    ← Cliente HTTP API Auditsa
│   ├── auditsa_tv_etl_service.py                  ← Orquestacion ETL TV
│   ├── auditsa_radio_etl_service.py               ← Orquestacion ETL Radio
│   ├── auditsa_impresos_etl_service.py            ← Orquestacion ETL Impresos
│   ├── auditsa_media_master_library_etl_service.py← ETL catalogo maestro
│   ├── auditsa_media_master_totals_etl_service.py ← ETL totales maestro
│   ├── auditsa_data_processor.py                  ← Validacion y procesamiento
│   ├── s3_service.py               ← Wrapper S3 (upload/download/delete)
│   ├── sqs_service.py              ← Wrapper SQS (send/receive/delete)
│   ├── sns_service.py              ← Wrapper SNS (publish a topics)
│   ├── event_store_service.py      ← CRUD Event Store + idempotencia
│   ├── dlq_service.py              ← Gestion de Dead Letter Queues
│   ├── log_service.py              ← Logging ETL a base de datos
│   ├── stats_cache_service.py      ← Cache in-memory con TTL de 10 min
│   ├── session_log_service.py      ← Audit log de queries (SELECT, EXPORT)
│   ├── account_sectors_service.py  ← Medios permitidos por cuenta
│   ├── sqlserver_write_service.py  ← Escritura dual a SQL Server mxTarifas
│   └── string_normalizer.py        ← Normalizacion de strings
│
└── tests/
    ├── conftest.py       ← Fixtures pytest
    ├── test_auth.py      ← Tests de autenticacion
    ├── test_health.py    ← Tests de health check
    ├── test_tv.py        ← Tests de endpoints TV
    └── test_errors.py    ← Tests de manejo de errores
```

---

## main.py — Entrypoint

FastAPI app v2.0.0. En el evento `startup`:

1. **`Base.metadata.create_all(bind=engine)`** — crea las tablas PostgreSQL si no existen.
2. **`_create_indexes()`** (background thread) — crea indices B-tree en `Fecha` y GIN trigrama (`pg_trgm`) en columnas de texto para acelerar autocompletado sobre tablas de 15-20 M de filas.
3. **`_prewarm_stats()`** (background thread) — pre-calcula y cachea los stats de las 3 tablas en `stats_cache_service`. Evita queries de ~40 s en el primer request del usuario.

Routers registrados: `auth`, `tv`, `radio`, `impresos`, `events`, `dlq`, `autocomplete`, `records`, `export`, `stats`, `admin`, `health`, `media_master_library`, `media_master_totals`.

Expone ademas `/metrics` (Prometheus via `prometheus-fastapi-instrumentator`).

---

## config/database.py

| Elemento | Descripcion |
|---|---|
| `Base` | `declarative_base()` compartido por todos los modelos PostgreSQL |
| `SqlServerBase` | `declarative_base()` para modelos SQL Server (mxTarifas) |
| `engine` | `create_engine(DATABASE_URL)` — PostgreSQL via `psycopg` |
| `sqlserver_engine_tarifas` | Engine para SQL Server mxTarifas (via `pymssql`) |
| `sqlserver_engine` | Engine para SQL Server generico / appSAC |
| `SessionLocal` | Fabrica de sesiones SQLAlchemy (PostgreSQL) |
| `SqlServerTarifasSessionLocal` | Fabrica de sesiones SQL Server mxTarifas |
| `SqlServerSacSessionLocal` | Fabrica de sesiones SQL Server appSAC |
| `get_db()` | Generador para inyeccion de dependencias (PostgreSQL) |
| `get_sqlserver_sac_db()` | Generador para inyeccion de dependencias (appSAC) |

---

## models/

### Tablas PostgreSQL (`Base`)

| Archivo | Clase | Tabla | Descripcion |
|---|---|---|---|
| `api_auditsa_tv.py` | `db_auditsa_tv` | `auditsa_api_tv` | Spots de TV (~53 columnas: plaza, canal, marca, campania, tarifa, etc.) |
| `api_auditsa_radio.py` | `db_auditsa_radio` | `auditsa_api_radio` | Spots de radio (mismo schema que TV) |
| `api_auditsa_impresos.py` | `db_auditsa_impresos` | `auditsa_api_impresos` | Avisos impresos (medio, fuente, autor, seccion, costo, dimension, etc.) |
| `api_auditsa_logs.py` | `db_auditsa_api_log` | `auditsa_api_log` | Log de operaciones ETL: source, instruccion, total_rows, status |
| `api_auditsa_media_master_library.py` | `db_auditsa_media_master_library` | `auditsa_api_media_master_library` | Catalogo de versiones/spots (IdMaster, TipoMaster, Anunciante, Marca, Industria) |
| `api_auditsa_media_master_totals.py` | `db_auditsa_media_master_totals` | `auditsa_api_media_master_totals` | Totales por periodo (FechaInicio, FechaFin, TotalHit, TotalInversion) |
| `domain_events.py` | `DomainEvent` | `domain_events` | Event Store inmutable: event_id (UUID), event_type, status (PENDING/PROCESSED/FAILED) |
| `catalogues_user_role.py` | `db_catalogue_user`, `db_catalogue_roles` | `catalogue_users`, `catalogue_roles` | Gestion de usuarios y roles en PostgreSQL |
| `catalogues_accounts.py` | — | — | Catalogo de cuentas en PostgreSQL |

### Tablas SQL Server (`SqlServerBase`)

| Archivo | Clase | Tabla | Descripcion |
|---|---|---|---|
| `sqlserver_auditsa.py` | `ss_auditsa_tv`, `ss_auditsa_radio`, `ss_auditsa_impresos` | `dbo.auditsa_api_*` | Replica de datos en SQL Server mxTarifas |
| `sqlserver_users.py` | `ss_catalogue_users`, `ss_catalogue_roles`, `ss_catalogue_accounts` | `catalogue.*` | Usuarios, roles y cuentas en SQL Server appSAC |

---

## routers/

Ver [routers/README.md](routers/README.md) para el detalle completo de endpoints.

**Resumen de prefijos:**

| Archivo | Prefijo | Proposito |
|---|---|---|
| `auth.py` | `/auth` | Login JWT, registro de usuarios, consulta de cuentas |
| `tv.py` | `/api/tv` | ETL, stats, status, delete para TV |
| `radio.py` | `/api/radio` | ETL, stats, status, delete para Radio |
| `impresos.py` | `/api/impresos` | ETL, stats, status, delete para Impresos |
| `events.py` | `/api/events` | Consulta y replay del Event Store |
| `dlq.py` | `/api/dlq` | Gestion de Dead Letter Queues |
| `autocomplete.py` | `/api/autocomplete` | Sugerencias cross-tabla con cascada |
| `records.py` | `/api/records` | Registros paginados con filtros |
| `export.py` | `/api/export` | Datos filtrados para exportacion Excel |
| `stats.py` | `/api/stats` | Stats batch de los 3 medios con cache |
| `admin.py` | `/admin` | Inicializacion SQL Server, invalidacion de cache |
| `health.py` | `/health` | Health check de SQL Server externo |
| `media_master_library.py` | `/api/media-master-library` | ETL y consultas del catalogo maestro |
| `media_master_totals.py` | `/api/media-master-totals` | ETL y consultas de totales por periodo |

---

## services/

Ver [services/README.md](services/README.md) para el detalle completo.

---

## Dockerfile

```dockerfile
FROM python:3.13-slim
RUN apt-get install -y gcc postgresql-client freetds-dev
WORKDIR /app
COPY requirements.txt . && pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Dependencias principales (`requirements.txt`)

| Paquete | Uso |
|---|---|
| `fastapi` | Framework API REST |
| `uvicorn[standard]` | Servidor ASGI |
| `sqlalchemy` | ORM multi-BD |
| `psycopg[binary]` | Driver PostgreSQL (psycopg3) |
| `pymssql` | Driver SQL Server |
| `pandas` | Transformaciones ETL |
| `boto3` | Cliente AWS (S3, SQS, SNS) |
| `pydantic` | Validacion de modelos |
| `pydantic-settings` | BaseSettings para variables de entorno |
| `python-jose[cryptography]` | JWT (HS256) |
| `passlib[bcrypt]` | Hash de contrasenas |
| `slowapi` | Rate limiting |
| `prometheus-fastapi-instrumentator` | Metricas en `/metrics` |
| `python-dotenv` | Carga de `.env` |

---

## Variables de entorno

### PostgreSQL

| Variable | Descripcion | Default |
|---|---|---|
| `DATABASE_URL` | URL completa (usada en Docker) | — |
| `POSTGRES_USER` | Usuario | — |
| `POSTGRES_PASSWORD` | Contrasena | — |
| `POSTGRES_HOST` | Host | `localhost` |
| `POSTGRES_PORT` | Puerto | `5432` |
| `POSTGRES_DB` | Base de datos | — |

### SQL Server

| Variable | Descripcion | Default |
|---|---|---|
| `SQLSERVER_HOST` | Host SQL Server externo | — |
| `SQLSERVER_PORT` | Puerto | `1433` |
| `SQLSERVER_USER` | Usuario | — |
| `SQLSERVER_PASSWORD` | Contrasena | — |
| `SQLSERVER_DB` | Base de datos principal | `master` |
| `SQLSERVER_DB_TARIFAS` | Base de datos mxTarifas | `mxTarifas` |

### AWS / LocalStack

| Variable | Descripcion | Default |
|---|---|---|
| `AWS_ENDPOINT_URL` | Endpoint LocalStack o AWS real | — |
| `AWS_ACCESS_KEY_ID` | Access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | Secret key | `test` |
| `AWS_REGION` | Region | `us-east-1` |
| `S3_BUCKET` | Bucket del Data Lake | `raw-data` |
| `SNS_BASE_ARN` | ARN base SNS | `arn:aws:sns:us-east-1:000000000000` |
| `SQS_BASE_URL` | URL base SQS | `http://localstack:4566/000000000000` |

### Auditsa API

| Variable | Descripcion |
|---|---|
| `AUDITSA_URL` | Base URL de la API |
| `AUDITSA_TOKEN` | Token de autenticacion |
| `AUDITSA_CLID` | Client ID |
| `STR_START` / `STR_END` | Sufijos de hora para parametros `perIni` / `perFin` |

### Autenticacion

| Variable | Descripcion | Default |
|---|---|---|
| `JWT_SECRET_KEY` | Clave secreta JWT | — |
| `JWT_ALGORITHM` | Algoritmo JWT | `HS256` |
| `JWT_EXPIRE_MINUTES` | Duracion del token | `1440` (24h) |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | Usuario admin de entorno | — |
| `READER_USERNAME` / `READER_PASSWORD` | Usuario reader de entorno | — |

### Comportamiento

| Variable | Descripcion | Default |
|---|---|---|
| `ETL_PARALLEL_WORKERS` | Workers para ETL de rango en paralelo | `5` |

---

## Ejecucion local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno (.env en la raiz del backend)
# Iniciar
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Documentacion interactiva disponible en `http://localhost:8000/docs`.
