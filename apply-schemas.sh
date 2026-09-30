#!/bin/bash

set -euo pipefail

# Load registry URL from .env if present, fall back to local default
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi
REGISTRY_URL="${CONFLUENT_SCHEMA_REGISTRY:-http://localhost:8081}"

for schema_file in ./schemas/*.avsc; do
  subject="$(basename "$schema_file" .avsc)"
  echo "Registering schema for subject '$subject' from $schema_file"

  # Validate + minify the schema JSON (fails loudly on invalid JSON)
  schema="$(jq -c '.' "$schema_file")"

  response="$(curl -sS -w $'\n%{http_code}' \
    -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$(jq -n --arg schema "$schema" '{schema: $schema}')" \
    "$REGISTRY_URL/subjects/$subject/versions")"

  body="$(sed '$d' <<<"$response")"
  http_code="$(tail -n1 <<<"$response")"

  if [[ "$http_code" != "200" ]]; then
    echo "Error registering '$subject': HTTP $http_code" >&2
    echo "$body" >&2
    exit 1
  fi
  echo "Registered '$subject' (id: $(jq -r '.id' <<<"$body"))"
done
