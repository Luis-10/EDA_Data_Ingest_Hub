#!/bin/bash
# verify_setup.sh — Verifica que todos los recursos EDA estén correctamente configurados
# Ejecutar después de que LocalStack haya procesado init.sh

set -e
echo "=============================================="
echo "  Verificación de recursos EDA en LocalStack"
echo "=============================================="

ERRORS=0

# ── S3 Buckets ─────────────────────────────────────────────────────────────────
echo ""
echo "[1/4] Verificando buckets S3..."
for bucket in raw-data processed-data; do
  if awslocal s3 ls "s3://$bucket" > /dev/null 2>&1; then
    echo "  ✓ s3://$bucket"
  else
    echo "  ✗ FALTA: s3://$bucket"
    ERRORS=$((ERRORS + 1))
  fi
done

# ── SNS Topics ────────────────────────────────────────────────────────────────
echo ""
echo "[2/4] Verificando topics SNS (Event Bus)..."
for topic in tv-events radio-events impresos-events; do
  ARN=$(awslocal sns list-topics --output json 2>/dev/null | python3 -c "
import json,sys
topics=json.load(sys.stdin).get('Topics',[])
matches=[t['TopicArn'] for t in topics if '$topic' in t['TopicArn']]
print(matches[0] if matches else '')
" 2>/dev/null)
  if [ -n "$ARN" ]; then
    echo "  ✓ $ARN"
  else
    echo "  ✗ FALTA topic: $topic"
    ERRORS=$((ERRORS + 1))
  fi
done

# ── SQS Queues ────────────────────────────────────────────────────────────────
echo ""
echo "[3/4] Verificando colas SQS de consumidores..."
for queue in tv-spark-queue radio-spark-queue impresos-spark-queue; do
  URL=$(awslocal sqs get-queue-url --queue-name "$queue" --output json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('QueueUrl',''))" 2>/dev/null)
  if [ -n "$URL" ]; then
    # Verificar mensajes pendientes
    MSGS=$(awslocal sqs get-queue-attributes --queue-url "$URL" --attribute-names ApproximateNumberOfMessages --output json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Attributes',{}).get('ApproximateNumberOfMessages','?'))" 2>/dev/null)
    echo "  ✓ $queue (mensajes pendientes: $MSGS)"
  else
    echo "  ✗ FALTA cola: $queue"
    ERRORS=$((ERRORS + 1))
  fi
done

# ── SNS Suscripciones ─────────────────────────────────────────────────────────
echo ""
echo "[4/4] Verificando suscripciones SNS → SQS..."
SUBS=$(awslocal sns list-subscriptions --output json 2>/dev/null | python3 -c "
import json,sys
subs=json.load(sys.stdin).get('Subscriptions',[])
for s in subs:
  topic=s['TopicArn'].split(':')[-1]
  endpoint=s['Endpoint'].split(':')[-1] if s['Endpoint'] != 'PendingConfirmation' else 'PendingConfirmation'
  print(f'  {topic} -> {endpoint} [{s[\"Protocol\"]}]')
" 2>/dev/null)

if [ -n "$SUBS" ]; then
  echo "$SUBS"
else
  echo "  ✗ No se encontraron suscripciones"
  ERRORS=$((ERRORS + 1))
fi

# ── Resultado ─────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
if [ $ERRORS -eq 0 ]; then
  echo "  RESULTADO: Todos los recursos EDA están OK"
  echo "  Flujo EDA activo:"
  echo "    S3 upload → S3 Notification → SNS Topic → SQS Queue → Spark Consumer"
else
  echo "  RESULTADO: $ERRORS recurso(s) faltante(s)"
  echo "  Revisar los logs de init.sh o reiniciar LocalStack."
fi
echo "=============================================="

exit $ERRORS