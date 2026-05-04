#!/usr/bin/env bash
# Phase 01 split E2E (Track D / T-D1) — Debezium PostgresConnector registration.
#
# Pre-req: docker compose up -d kafka schema-registry kafka-connect.
# Waits for Kafka Connect REST + connector plugin install before POSTing.

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:18083}"
CONFIG_FILE="$(cd "$(dirname "$0")" && pwd)/pg-source-connector.json"
CONNECTOR_NAME="cdc-pg-source"

echo "→ Waiting for Kafka Connect REST at ${CONNECT_URL} ..."
for i in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "${CONNECT_URL}/" >/dev/null

echo "→ Waiting for PostgresConnector plugin to be installed ..."
for i in $(seq 1 90); do
  if curl -fsS "${CONNECT_URL}/connector-plugins" \
      | grep -q 'io.debezium.connector.postgresql.PostgresConnector'; then
    echo "   plugin present."
    break
  fi
  sleep 2
done

if ! curl -fsS "${CONNECT_URL}/connector-plugins" \
    | grep -q 'io.debezium.connector.postgresql.PostgresConnector'; then
  echo "✗ PostgresConnector plugin not installed within timeout"
  curl -sS "${CONNECT_URL}/connector-plugins" | tr ',' '\n' | head -20
  exit 1
fi

echo "→ Deleting any pre-existing connector ${CONNECTOR_NAME} ..."
curl -sS -o /dev/null -w "  delete status: %{http_code}\n" \
  -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" || true

echo "→ Registering ${CONNECTOR_NAME} from ${CONFIG_FILE} ..."
curl -fsS -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  | sed 's/^/  /'

echo
echo "→ Waiting up to 60s for connector + task to reach RUNNING ..."
for i in $(seq 1 30); do
  STATUS_JSON=$(curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true)
  CONN_STATE=$(echo "${STATUS_JSON}" | grep -o '"connector":{"state":"[^"]*"' | head -1 | sed 's/.*"state":"\([^"]*\)".*/\1/')
  TASK_STATE=$(echo "${STATUS_JSON}" | grep -o '"tasks":\[{[^}]*' | grep -o '"state":"[^"]*"' | head -1 | sed 's/"state":"\([^"]*\)"/\1/')
  echo "  [${i}] connector=${CONN_STATE:-?} task=${TASK_STATE:-?}"
  if [[ "${CONN_STATE}" == "RUNNING" && "${TASK_STATE}" == "RUNNING" ]]; then
    echo "✓ ${CONNECTOR_NAME} is RUNNING"
    exit 0
  fi
  if [[ "${CONN_STATE}" == "FAILED" || "${TASK_STATE}" == "FAILED" ]]; then
    echo "✗ Connector reported FAILED:"
    echo "${STATUS_JSON}"
    exit 1
  fi
  sleep 2
done

echo "✗ Connector did not reach RUNNING in time"
echo "${STATUS_JSON}"
exit 1
