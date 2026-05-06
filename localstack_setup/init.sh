#!/bin/bash
set -e

echo "=============================================="
echo "  Inicializando recursos AWS (EDA Mode)"
echo "=============================================="

# ── 1. BUCKETS S3 ──────────────────────────────────────────────────────────────
echo "[S3] Creando buckets..."
awslocal s3 mb s3://raw-data
awslocal s3 mb s3://processed-data

# Placeholders para crear estructura de carpetas
echo "" > /tmp/.keep
awslocal s3api put-object --bucket raw-data --key rawdata/tv/.keep       --body /tmp/.keep
awslocal s3api put-object --bucket raw-data --key rawdata/radio/.keep    --body /tmp/.keep
awslocal s3api put-object --bucket raw-data --key rawdata/impresos/.keep --body /tmp/.keep
echo "[S3] Buckets creados."

# ── 2. SNS TOPICS (Event Bus central) ─────────────────────────────────────────
echo "[SNS] Creando topics (Event Bus)..."
awslocal sns create-topic --name tv-events
awslocal sns create-topic --name radio-events
awslocal sns create-topic --name impresos-events
echo "[SNS] Topics creados."

# ── 3. DEAD LETTER QUEUES (DLQ) ────────────────────────────────────────────────
echo "[DLQ] Creando Dead Letter Queues..."
awslocal sqs create-queue --queue-name tv-spark-dlq
awslocal sqs create-queue --queue-name radio-spark-dlq
awslocal sqs create-queue --queue-name impresos-spark-dlq
echo "[DLQ] DLQs creadas."

# Obtener URLs y ARNs de los DLQs
TV_DLQ_URL=$(awslocal sqs get-queue-url --queue-name tv-spark-dlq --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")
RADIO_DLQ_URL=$(awslocal sqs get-queue-url --queue-name radio-spark-dlq --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")
IMPRESOS_DLQ_URL=$(awslocal sqs get-queue-url --queue-name impresos-spark-dlq --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")

TV_DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$TV_DLQ_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")
RADIO_DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$RADIO_DLQ_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")
IMPRESOS_DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$IMPRESOS_DLQ_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")

# ── 4. SQS SPARK QUEUES con RedrivePolicy → DLQ (maxReceiveCount=3) ───────────
# VisibilityTimeout=300 (5 min): evita que mensajes se reprocesen si Spark tarda más que el default (30s).
# MessageRetentionPeriod=86400 (24h): retiene mensajes por 1 día.
echo "[SQS] Creando colas Spark con RedrivePolicy + VisibilityTimeout..."
awslocal sqs create-queue --queue-name tv-spark-queue \
  --attributes "$(python3 -c "import json; print(json.dumps({'VisibilityTimeout': '300', 'MessageRetentionPeriod': '86400', 'RedrivePolicy': json.dumps({'deadLetterTargetArn': '${TV_DLQ_ARN}', 'maxReceiveCount': 3})}))")"

awslocal sqs create-queue --queue-name radio-spark-queue \
  --attributes "$(python3 -c "import json; print(json.dumps({'VisibilityTimeout': '300', 'MessageRetentionPeriod': '86400', 'RedrivePolicy': json.dumps({'deadLetterTargetArn': '${RADIO_DLQ_ARN}', 'maxReceiveCount': 3})}))")"

awslocal sqs create-queue --queue-name impresos-spark-queue \
  --attributes "$(python3 -c "import json; print(json.dumps({'VisibilityTimeout': '300', 'MessageRetentionPeriod': '86400', 'RedrivePolicy': json.dumps({'deadLetterTargetArn': '${IMPRESOS_DLQ_ARN}', 'maxReceiveCount': 3})}))")"

echo "[SQS] Colas Spark creadas con RedrivePolicy (max 3 intentos → DLQ) + VisibilityTimeout=300s."

# ── 5. SUSCRIPCION SQS → SNS (fan-out) ────────────────────────────────────────
echo "[SNS->SQS] Suscribiendo colas a los topics..."

TV_TOPIC_ARN=$(awslocal sns list-topics --output json | python3 -c "import json,sys; topics=json.load(sys.stdin)['Topics']; print([t['TopicArn'] for t in topics if 'tv-events' in t['TopicArn']][0])")
RADIO_TOPIC_ARN=$(awslocal sns list-topics --output json | python3 -c "import json,sys; topics=json.load(sys.stdin)['Topics']; print([t['TopicArn'] for t in topics if 'radio-events' in t['TopicArn']][0])")
IMPRESOS_TOPIC_ARN=$(awslocal sns list-topics --output json | python3 -c "import json,sys; topics=json.load(sys.stdin)['Topics']; print([t['TopicArn'] for t in topics if 'impresos-events' in t['TopicArn']][0])")

TV_QUEUE_URL=$(awslocal sqs get-queue-url --queue-name tv-spark-queue --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")
RADIO_QUEUE_URL=$(awslocal sqs get-queue-url --queue-name radio-spark-queue --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")
IMPRESOS_QUEUE_URL=$(awslocal sqs get-queue-url --queue-name impresos-spark-queue --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['QueueUrl'])")

TV_QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$TV_QUEUE_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")
RADIO_QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$RADIO_QUEUE_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")
IMPRESOS_QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$IMPRESOS_QUEUE_URL" --attribute-names QueueArn --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['Attributes']['QueueArn'])")

awslocal sns subscribe --topic-arn "$TV_TOPIC_ARN"       --protocol sqs --notification-endpoint "$TV_QUEUE_ARN"
awslocal sns subscribe --topic-arn "$RADIO_TOPIC_ARN"    --protocol sqs --notification-endpoint "$RADIO_QUEUE_ARN"
awslocal sns subscribe --topic-arn "$IMPRESOS_TOPIC_ARN" --protocol sqs --notification-endpoint "$IMPRESOS_QUEUE_ARN"
echo "[SNS->SQS] Suscripciones creadas."

# ── 6. S3 EVENT NOTIFICATIONS → SNS ───────────────────────────────────────────
echo "[S3->SNS] Configurando notificaciones de S3 hacia SNS..."

cat > /tmp/bucket-notification.json << EOF
{
  "TopicConfigurations": [
    {
      "Id": "tv-data-ingested",
      "TopicArn": "${TV_TOPIC_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "rawdata/tv/"}]
        }
      }
    },
    {
      "Id": "radio-data-ingested",
      "TopicArn": "${RADIO_TOPIC_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "rawdata/radio/"}]
        }
      }
    },
    {
      "Id": "impresos-data-ingested",
      "TopicArn": "${IMPRESOS_TOPIC_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "rawdata/impresos/"}]
        }
      }
    }
  ]
}
EOF

awslocal s3api put-bucket-notification-configuration \
  --bucket raw-data \
  --notification-configuration file:///tmp/bucket-notification.json

echo "[S3->SNS] Notificaciones configuradas."

# ── RESUMEN ────────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo "  Recursos EDA + DLQ listos"
echo "=============================================="
echo "[S3] Buckets:"
awslocal s3 ls

echo ""
echo "[SNS] Topics (Event Bus):"
awslocal sns list-topics --output json | python3 -c "import json,sys; [print('  ' + t['TopicArn']) for t in json.load(sys.stdin)['Topics']]"

echo ""
echo "[SQS] Todas las colas (spark + DLQ):"
awslocal sqs list-queues --output json | python3 -c "import json,sys; data=json.load(sys.stdin); [print('  ' + u) for u in data.get('QueueUrls',[])]"

echo ""
echo "Flujo EDA activo:"
echo "  S3 upload rawdata/tv/*  → SNS tv-events → SQS tv-spark-queue → Spark"
echo "  S3 upload rawdata/radio/* → SNS radio-events → SQS radio-spark-queue → Spark"
echo "  S3 upload rawdata/impresos/* → SNS impresos-events → SQS impresos-spark-queue → Spark"
echo ""
echo "Flujo de mensajes fallidos (DLQ):"
echo "  tv-spark-queue    (3 fallos) → tv-spark-dlq"
echo "  radio-spark-queue (3 fallos) → radio-spark-dlq"
echo "  impresos-spark-queue (3 fallos) → impresos-spark-dlq"