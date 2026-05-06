# monitoring — Stack de Observabilidad

Stack de observabilidad del pipeline EDA compuesto por **Prometheus** (recoleccion de metricas) y **Grafana** (visualizacion). Ambos servicios se levantan junto al resto del stack via `docker-compose.yml`.

---

## Estructura

```
monitoring/
├── prometheus/
│   └── prometheus.yml              ← Configuracion de scraping
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── prometheus.yml      ← Datasource auto-provisionado
    │   └── dashboards/
    │       └── dashboards.yml      ← Config de carga automatica de dashboards
    └── dashboards/
        └── eda-ingestion-monitor.json  ← Dashboard pre-configurado del pipeline
```

---

## Servicios

### Prometheus

| Parametro | Valor |
|---|---|
| Imagen | `prom/prometheus:latest` |
| Puerto | `9090` |
| Configuracion | `monitoring/prometheus/prometheus.yml` |
| Almacenamiento | Volumen `prometheus_data` (persistente) |
| Web Lifecycle API | Habilitada (`--web.enable-lifecycle`) |

**Acceso:** `http://localhost:9090`

#### `prometheus/prometheus.yml`

Define dos jobs de scraping:

| Job | Target | Path | Intervalo |
|---|---|---|---|
| `fastapi-backend` | `backend:8000` | `/metrics` | 15 s |
| `prometheus` | `localhost:9090` | `/metrics` (default) | 15 s |

Las metricas del backend son expuestas por `prometheus-fastapi-instrumentator` e incluyen:
- Conteo y duracion de requests HTTP por endpoint, metodo y codigo de estado
- Metricas de proceso Python (CPU, memoria, GC)

---

### Grafana

| Parametro | Valor |
|---|---|
| Imagen | `grafana/grafana:latest` |
| Puerto host | `3001` (mapeado a `3000` interno — no conflicta con el frontend en `:3000`) |
| Credenciales | user: `admin` / password: `admin123` |
| Provisioning | `monitoring/grafana/provisioning/` |
| Almacenamiento | Volumen `grafana_data` (persistente) |

**Acceso:** `http://localhost:3001`

Plugins preinstalados: `grafana-clock-panel`, `grafana-simple-json-datasource`, `grafana-piechart-panel`.

#### `grafana/provisioning/datasources/prometheus.yml`

Provisiona automaticamente el datasource de Prometheus al arrancar Grafana (sin configuracion manual):

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

El campo `editable: false` impide modificar el datasource desde la UI, garantizando que la configuracion siempre se gestione por codigo.

#### `grafana/dashboards/eda-ingestion-monitor.json`

Dashboard pre-configurado del pipeline EDA. Se carga automaticamente al arrancar Grafana. Incluye paneles para:
- Tasa de requests HTTP por endpoint y codigo de estado
- Latencia p95 por handler
- Errores 5xx en tiempo real
- Uso de CPU y memoria del backend

---

## Metricas disponibles (FastAPI)

`prometheus-fastapi-instrumentator` expone automaticamente en `/metrics`:

| Metrica | Descripcion |
|---|---|
| `http_requests_total` | Total de requests por metodo, handler y codigo de estado |
| `http_request_duration_seconds` | Histograma de duracion de requests |
| `http_requests_in_progress` | Requests en curso (gauge) |
| `process_cpu_seconds_total` | CPU consumido por el proceso |
| `process_resident_memory_bytes` | Memoria residente |

Ejemplo de queries PromQL utiles para el dashboard:

```promql
# Tasa de requests por endpoint (ultimos 5 min)
rate(http_requests_total[5m])

# Latencia p95 por handler
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Total de errores 5xx
sum(rate(http_requests_total{status_code=~"5.."}[5m]))
```

---

## Agregar un dashboard

1. Crear el dashboard en la UI de Grafana (`http://localhost:3001`).
2. Exportarlo como JSON: **Dashboard → Share → Export → Save to file**.
3. Guardarlo en `monitoring/grafana/dashboards/`.
4. Reiniciar Grafana para que lo cargue automaticamente desde el volumen provisionado.

---

## Diagrama de conexiones

```
FastAPI backend (:8000)
  └─► GET /metrics
        │
        ▼
  Prometheus (:9090)
    scrape cada 15s
        │
        ▼
  Grafana (:3001)
    datasource: http://prometheus:9090
    dashboard: eda-ingestion-monitor.json
```

Todos los servicios se comunican dentro de la red Docker `data-network`.
