# Routers — Capa de API (FastAPI)

Contiene los routers FastAPI que exponen la API REST del pipeline EDA. Cada router delega la logica en los servicios correspondientes e implementa el flujo event-driven descrito en la arquitectura.

---

## Flujo general de ingesta (POST /etl)

```
POST /api/{tipo}/etl
  1. Extract   → AuditsaApiService (datos crudos desde Auditsa)
  2. S3 Upload → S3Service (rawdata/{tipo}/{fecha}/data.json)
               → S3 Event Notification dispara SNS automaticamente
               → SNS hace fan-out a {tipo}-spark-queue
               → Spark Consumer procesa asincronamente
  3. EventStore → EventStoreService.publish(DataIngested)
  4. DualWrite  → SqlServerWriteService.upsert (graceful degradation)
  5. Log        → LogService.start() / complete()
  6. Retorna 202 Accepted + event_id
```

**DELETE** sigue el flujo inverso: elimina objetos S3 → elimina registros en BD → publica `DataDeleted` en SNS → registra en Event Store.

---

## Archivos

### Router de autenticacion (`auth.py`)

Prefijo: `/auth` | Tag: `Auth`

| Metodo | Ruta | Descripcion | Rol |
|---|---|---|---|
| `GET` | `/accounts` | Lista de cuentas disponibles desde SQL Server appSAC. Usado para poblar el dropdown de registro. | — |
| `POST` | `/token` | Obtener token JWT. Verifica primero usuarios de entorno (`.env`), luego usuarios registrados en DB. Incluye `allowed_media` en la respuesta. | — |
| `POST` | `/register` | Registrar nuevo usuario con rol `standard`. Dual-write a PostgreSQL + SQL Server appSAC. | — |
| `GET` | `/me` | Datos del usuario autenticado (username, rol, allowed_media). | cualquiera |

---

### Routers de medios

Los tres routers de medios son simetricos. Exponen el mismo conjunto de endpoints con prefijos distintos.

| Archivo | Prefijo | Tag | SNS Topic | Endpoint Auditsa |
|---|---|---|---|---|
| `tv.py` | `/api/tv` | `TV` | `tv-events` | `/GetHits` |
| `radio.py` | `/api/radio` | `Radio` | `radio-events` | `/GetHitsAutomatico` |
| `impresos.py` | `/api/impresos` | `Impresos` | `impresos-events` | `/GetPrintedHits` |

#### Endpoints comunes (TV, Radio e Impresos)

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/stats` | Estadisticas globales (total registros, fechas, canales, marcas, anunciantes, tarifa). Con cache en memoria (TTL 10 min). |
| `GET` | `/top-marcas` | Top N marcas por inversion total. Con cache. |
| `GET` | `/top-marcas/range` | Top N marcas en un rango de fechas con filtros opcionales (anunciante, marca, medio, tipo_medio, localidad). |
| `GET` | `/status/range` | Estadisticas filtradas para un rango de fechas. |
| `GET` | `/{fecha}` | Datos crudos desde la API de Auditsa para una fecha (sin ETL). |
| `GET` | `/range/{fecha_inicio}/{fecha_fin}` | Datos crudos para un rango de fechas. |
| `GET` | `/status/{fecha}` | Estado de registros en BD para una fecha. |
| `POST` | `/etl` | Ejecuta ETL (Extract + S3 upload + EventStore + DualWrite). Body: `{"fecha": "YYYYMMDD"}`. |
| `POST` | `/etl/range` | ETL en paralelo para un rango de fechas (`ETL_PARALLEL_WORKERS` workers, default 5). Body: `{"fecha_inicio": "YYYYMMDD", "fecha_fin": "YYYYMMDD"}`. |
| `DELETE` | `/{fecha}` | Elimina datos de S3 y BD para una fecha. Publica `DataDeleted` en SNS. |
| `DELETE` | `/range` | Elimina datos de S3 y BD para un rango. |

#### Endpoints exclusivos de Impresos

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/records` | Registros paginados con filtros (fecha_inicio, fecha_fin, medio, fuente, anunciante, marca, sector). |
| `POST` | `/fix-nulls` | Normaliza NULLs historicos en `auditsa_api_impresos`. |

> **Nota sobre filtros:** TV y Radio no tienen `Categoria`; Impresos no tiene `Localidad` ni `TipoMedio`.

---

### Router de Records (`records.py`)

Prefijo: `/api/records` | Tag: `Records`

Registros paginados para las tablas de detalle del dashboard. Excluye automaticamente columnas `Id*` para mostrar solo datos descriptivos.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/{tipo}` | Registros paginados de tv, radio o impresos. Parametros: `fecha_inicio`, `fecha_fin`, `page` (default 1), `page_size` (10-200, default 25), filtros: `anunciante`, `marca`, `submarca`, `medio`, `tipo_medio`, `localidad`, `categoria`, `sector_industria`, `industria`. |

Respuesta: `{ tipo, columns, records, total, page, page_size, pages }`.

Registra cada consulta en auditoria via `session_log_service.log_action` (BackgroundTask).

Respeta `allowed_media` del token: si el usuario no tiene acceso al tipo solicitado, retorna 403.

---

### Router de Export (`export.py`)

Prefijo: `/api/export` | Tag: `Export`

Exportacion de datos filtrados para generar archivos Excel en el frontend. Soporta seleccion dinamica de columnas.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/{tipo}` | Datos filtrados para exportacion. Parametros: `fecha_inicio`, `fecha_fin`, `columns` (CSV obligatorio), filtros por anunciante/marca/medio/etc. Maximo 1,000,000 filas. |

La columna especial `Ins` se genera como literal `1` (conteo de registros).

Respuesta: `{ tipo, total_available, total_exported, truncated, columns, records }`.

Registra cada exportacion en auditoria via `session_log_service.log_action` (BackgroundTask).

---

### Router de Stats batch (`stats.py`)

Prefijo: `/api/stats` | Tag: `Stats`

Endpoint que devuelve stats + top-marcas + top-sectores de los 3 medios en una sola llamada, leyendo desde cache en memoria.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/summary` | Stats + top-10 marcas + top-10 sectores de cada medio en una sola respuesta. Filtra por `allowed_media` del token. Cada metrica se lee de cache independientemente; solo computa las que faltan. |

---

### Router de Admin (`admin.py`)

Prefijo: `/admin` | Tag: `Admin`

Operaciones administrativas que requieren rol `admin`.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `POST` | `/sqlserver/init-tables` | Crea tablas de ingesta en SQL Server mxTarifas (`dbo.auditsa_api_tv`, `dbo.auditsa_api_radio`, `dbo.auditsa_api_impresos`). Idempotente. |
| `POST` | `/cache/refresh` | Invalida el cache de stats en memoria y recalcula los valores desde la DB. Util cuando el cache aun no expiro (TTL 10 min) y se ingesto nueva data. |

---

### Router de Health (`health.py`)

Prefijo: `/health` | Tag: `Health`

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/sqlserver` | Verifica conectividad con SQL Server externo. Retorna 200 con latencia en ms, nombre del servidor y version; o 503 si falla. |

---

### Router de Events (`events.py`)

Prefijo: `/api/events` | Tag: `Events`

API de consulta y gestion del Event Store (`domain_events`).

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/` | Lista eventos con filtros opcionales: `tipo`, `fecha`, `event_type` (DataIngested/DataProcessed/DataDeleted), `status` (PENDING/PROCESSED/FAILED), `limit`. |
| `GET` | `/pending` | Atajo para eventos en estado `PENDING`. Filtro opcional por `tipo`. |
| `GET` | `/{event_id}` | Detalle de un evento por su UUID. |
| `POST` | `/{event_id}/replay` | Reencola un evento `DataIngested` al topic SNS correspondiente y resetea su estado a `PENDING`. |

---

### Router DLQ (`dlq.py`)

Prefijo: `/api/dlq` | Tag: `DLQ`

Gestion de Dead Letter Queues con integracion al Event Store.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/` | Stats de los 3 DLQs (tv/radio/impresos): mensajes disponibles, en vuelo y `domain_events` FAILED. |
| `GET` | `/{tipo}` | Lista mensajes del DLQ del tipo indicado. Detecta envelope SNS automaticamente. |
| `POST` | `/{tipo}/replay` | Mueve mensajes del DLQ a la cola Spark principal y resetea `domain_events` FAILED → PENDING. |
| `DELETE` | `/{tipo}` | Purga permanentemente el DLQ. Los `domain_events` FAILED quedan como registro historico. |

---

### Router Autocomplete (`autocomplete.py`)

Prefijo: `/api/autocomplete` | Tag: `Autocomplete`

Sugerencias para los filtros del dashboard con logica de cascada cross-tabla.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/` | Retorna sugerencias para el campo `field`. Parametros: `q` (texto libre), `limit` (1-50), filtros en cascada: `anunciante`, `marca`, `medio`, `tipo_medio`, `localidad`, `categoria`, `fuente`, `sector`, `fecha_inicio`, `fecha_fin`. |

**Campos disponibles:** `Anunciante`, `Marca`, `Medio`, `TipoMedio`, `Localidad` (TV + Radio), `Categoria`, `Fuente`, `Sector` (solo Impresos).

**Logica de cascada:** si un filtro activo no existe en una tabla (ej. `categoria` no existe en TV), esa tabla se omite de la consulta. Las 3 tablas se unen para campos comunes (`Anunciante`, `Marca`, `Medio`).

---

### Routers de Media Master

#### `media_master_library.py`

Prefijo: `/api/media-master-library` | Tag: `Media Master Library`

Catalogo de versiones/spots de Auditsa. Carga directa sin S3/Spark: API → Transform → PostgreSQL.

| Metodo | Ruta | Descripcion |
|---|---|---|
| `GET` | `/stats` | Estadisticas generales del catalogo (total masters, tipos, anunciantes, marcas, industrias). |
| `GET` | `/status/{fecha}` | Estado de registros para una fecha. |
| `GET` | `/{fecha}` | Datos crudos de la API para una fecha. |
| `POST` | `/etl` | Ingesta directa del catalogo. |
| `POST` | `/etl/range` | Ingesta de rango en paralelo. |
| `DELETE` | `/{fecha}` | Elimina registros para una fecha. |

#### `media_master_totals.py`

Prefijo: `/api/media-master-totals` | Tag: `Media Master Totals`

Totales agregados por periodo. Misma estructura que Media Master Library.

---

## Modelos Pydantic (schemas.py)

Usados en los routers de medios:

```python
class ETLRequest(BaseModel):
    fecha: str          # Formato YYYYMMDD

class ETLRangeRequest(BaseModel):
    fecha_inicio: str   # Formato YYYYMMDD
    fecha_fin: str      # Formato YYYYMMDD

FechaQuery = Annotated[str, Query(pattern=r"^\d{8}$")]  # Validacion de formato en path/query params
```

---

## Variables de entorno relevantes

| Variable | Uso |
|---|---|
| `ETL_PARALLEL_WORKERS` | Numero de workers para ETL de rango (default: `5`) |
| `JWT_SECRET_KEY` | Clave secreta para firmar tokens JWT |
| `SQLSERVER_HOST` | Host SQL Server para dual-write y health check |
