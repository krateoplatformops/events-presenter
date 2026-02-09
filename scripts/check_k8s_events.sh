#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=demo-system
DB_POD=postgres-57c75dfd6f-2qrst

DB_NAME=testdb
DB_USER=test
DB_PASS=test

# Queries più leggibili
QUERIES=$(cat <<'SQL'
-- Mostra partizioni
SELECT inhrelid::regclass AS partition
FROM pg_inherits
WHERE inhparent = 'k8s_events'::regclass;

-- Mostra 10 eventi chiave con colonne importanti
SELECT created_at, namespace, resource_kind, resource_name, event_type, reason, uid
FROM k8s_events
ORDER BY created_at DESC
LIMIT 10;

-- Conta tutti gli eventi
SELECT count(*) FROM k8s_events;
SQL
)

echo "Eseguo queries su pod $DB_POD nel namespace $NAMESPACE..."

kubectl exec -i -n "$NAMESPACE" "$DB_POD" \
  -- env PGPASSWORD="$DB_PASS" psql -U "$DB_USER" -d "$DB_NAME" \
  -c "\x on" \
  -c "$QUERIES"
