# backend/models/

Modelos ORM de SQLAlchemy que representan las tablas del Data Warehouse en PostgreSQL (`schema: public`).
También incluye modelos para la base de datos SQL Server (`schema: dbo` - default).

Todos los modelos heredan de `Base` (importado desde `config.database`) para que `Base.metadata.create_all()` los registre automáticamente al iniciar la aplicación.

## Archivos

### `api_auditsa_tv.py` — Tabla `auditsa_api_tv`

Almacena los spots de televisión detectados por Auditsa. Cada fila representa un spot emitido.

Columnas clave:

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | Integer PK | Autoincremental |
| `Fecha` | Date | Fecha de emisión del spot |
| `Canal` | String(255) | Canal de TV |
| `Marca` | String(255) | Marca anunciante |
| `Anunciante` | String(255) | Anunciante |
| `Campaña` | String(255) | Nombre de campaña |
| `DReal` | Integer | Duración real en segundos |
| `DTeorica` | Integer | Duración teórica en segundos |
| `Tarifa` | Float | Tarifa del spot |
| `IdDeteccion` | BigInteger | ID único de detección en Auditsa |
| `HInicio` / `HFinal` | String(50) | Hora de inicio y fin del spot |

### `api_auditsa_radio.py` — Tabla `auditsa_api_radio`

Almacena los spots de radio detectados automáticamente. Tiene el mismo esquema de columnas que `auditsa_api_tv` (incluye `Canal` para la estación de radio, `IdDeteccion`, `Tarifa`, etc.).

### `api_auditsa_impresos.py` — Tabla `auditsa_api_impresos`

Almacena los avisos de medios impresos (prensa, revistas). Esquema diferenciado:

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | Integer PK | Autoincremental |
| `Fecha` | Date | Fecha del aviso |
| `Medio` | String(255) | Medio impreso |
| `Fuente` | String | Fuente del dato |
| `Seccion` | String | Sección del medio |
| `Pagina` | String | Página |
| `Anunciante` | String(255) | Anunciante |
| `Marca` | String(255) | Marca |
| `Tiraje` | BigInteger | Tiraje del medio |
| `Costo` | Float | Costo del aviso |
| `Dimension` | Float | Dimensión en cm² |

### `api_auditsa_logs.py` — Tabla `auditsa_api_log`

Registro de auditoría de todas las operaciones ETL realizadas por el sistema. Cada operación genera al menos dos entradas: una para S3 y otra para PostgreSQL.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | Integer PK | Autoincremental |
| `source` | String(500) | Origen/destino: URI de S3 (`s3://...`) o nombre de tabla (`public.auditsa_api_tv`) |
| `date_time` | DateTime | Timestamp UTC de inicio de la operación |
| `instruction` | String(50) | Tipo de operación: `INSERT` o `DELETE` |
| `total_rows` | BigInteger | Total de filas disponibles en la fuente (API) |
| `total_records` | BigInteger | Filas efectivamente procesadas/cargadas |
| `status` | String(50) | Estado: `RECORDING` (en progreso) → `SUCCESS` (completado) |

#### Ciclo de vida de un log

```
log.start(source, instruction, total_rows)  →  status = "RECORDING"
        [operación en curso]
log.complete(log_id, total_records)          →  status = "SUCCESS"
```

## Registro en el startup

Los modelos se importan explícitamente en `main.py` para que `Base` los conozca antes de ejecutar `create_all`:

```python
import models.api_auditsa_tv
import models.api_auditsa_radio
import models.api_auditsa_impresos
import models.api_auditsa_logs
```
