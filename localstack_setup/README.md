# localstack_setup — Inicialización de recursos AWS locales

Scripts de configuración que se ejecutan al arrancar el contenedor de **LocalStack** para aprovisionar todos los recursos AWS necesarios para el pipeline EDA en entorno de desarrollo.

---

## Archivos

| Archivo           | Descripción                                                                                                                        |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `init.sh`         | Crea y configura todos los recursos AWS (S3, SNS, SQS, notificaciones). Se monta como `docker-entrypoint-initaws.d` en LocalStack. |
| `verify_setup.sh` | Verifica que todos los recursos estén correctamente creados. Útil para diagnóstico post-arranque.                                  |

---

## Recursos creados por `init.sh`

El script se ejecuta en 6 pasos en orden de dependencia:

### 1. Buckets S3

| Bucket           | Propósito                                                           |
| ---------------- | ------------------------------------------------------------------- |
| `raw-data`       | Data Lake — almacena los JSON crudos extraídos de la API de Auditsa |
| `processed-data` | Bucket de datos procesados (reservado para uso futuro)              |

Estructura de prefijos creada en `raw-data`:

```
rawdata/
  tv/
  radio/
  impresos/
```

### 2. Topics SNS (Event Bus central)

| Topic             | Disparado por                                       |
| ----------------- | --------------------------------------------------- |
| `tv-events`       | Upload a `rawdata/tv/*` o DELETE desde la API       |
| `radio-events`    | Upload a `rawdata/radio/*` o DELETE desde la API    |
| `impresos-events` | Upload a `rawdata/impresos/*` o DELETE desde la API |

### 3. Dead Letter Queues (DLQ)

Colas SQS simples que reciben los mensajes que fallaron 3 veces en la cola principal de Spark.

| Cola DLQ             |
| -------------------- |
| `tv-spark-dlq`       |
| `radio-spark-dlq`    |
| `impresos-spark-dlq` |

### 4. Colas SQS Spark con RedrivePolicy

Colas principales que consumen los eventos del pipeline. Cada cola tiene configurado un **RedrivePolicy** que redirige al DLQ correspondiente tras `maxReceiveCount=3` intentos fallidos.

| Cola principal         | DLQ asociado         | Max intentos |
| ---------------------- | -------------------- | ------------ |
| `tv-spark-queue`       | `tv-spark-dlq`       | 3            |
| `radio-spark-queue`    | `radio-spark-dlq`    | 3            |
| `impresos-spark-queue` | `impresos-spark-dlq` | 3            |

### 5. Suscripciones SNS → SQS (fan-out)

Cada cola Spark queda suscrita a su topic SNS correspondiente:

```
tv-events       → tv-spark-queue
radio-events    → radio-spark-queue
impresos-events → impresos-spark-queue
```

### 6. S3 Event Notifications → SNS

Configuradas sobre el bucket `raw-data`. Cada prefijo dispara el topic SNS correspondiente ante el evento `s3:ObjectCreated:*`:

| Prefijo S3          | Topic SNS         |
| ------------------- | ----------------- |
| `rawdata/tv/`       | `tv-events`       |
| `rawdata/radio/`    | `radio-events`    |
| `rawdata/impresos/` | `impresos-events` |

---

## Flujo completo resultante

```
POST /api/{tipo}/etl (FastAPI)
  └─► S3 upload: rawdata/{tipo}/{fecha}/data.json
        └─► S3 Event Notification (s3:ObjectCreated:*)
              └─► SNS Topic: {tipo}-events
                    └─► SQS Queue: {tipo}-spark-queue  ←── Spark Consumer lee aquí
                                        │
                                   3 fallos
                                        │
                                        ▼
                              SQS DLQ: {tipo}-spark-dlq  ←── /api/dlq gestiona replay
```

---

## `verify_setup.sh` — Verificación

Comprueba en 4 pasos que todos los recursos existen y están conectados:

1. **Buckets S3** — verifica `raw-data` y `processed-data`
2. **Topics SNS** — verifica los 3 topics del Event Bus
3. **Colas SQS** — verifica las 3 colas Spark e imprime el conteo de mensajes pendientes
4. **Suscripciones SNS → SQS** — lista todas las suscripciones activas

Retorna código de salida `0` si todo está OK, o el número de recursos faltantes si hay errores.

### Uso

```bash
# Ejecutar desde dentro del contenedor LocalStack
docker exec <nombre-contenedor-localstack> bash /etc/localstack/init/ready.d/verify_setup.sh

# O directamente si LocalStack está accesible
bash localstack_setup/verify_setup.sh
```

---

## Integración con docker-compose

El `init.sh` se monta en el directorio `docker-entrypoint-initaws.d` de LocalStack para que se ejecute automáticamente al arrancar el contenedor:

```yaml
localstack:
  image: localstack/localstack
  volumes:
    - ./localstack_setup/init.sh:/etc/localstack/init/ready.d/init.sh
```

Los recursos se recrean en cada reinicio de LocalStack (los datos son efímeros en entorno local).
