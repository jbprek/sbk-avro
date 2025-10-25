#!/bin/bash
set -e

BROKER=broker:29092
TOPIC1=birth.register.avro
TOPIC2=birth.stats.avro
SCHEMA_REGISTRY_URL=http://schema-registry:8081
SCHEMA_FILE=/schemas/birth-schema.avsc
SCHEMA_FILE2=/schemas/birth-stat-entry.avsc

# Wait for broker
echo "Waiting for Kafka broker at $BROKER..."
while ! nc -z broker 29092; do
  sleep 2
done
echo "Kafka broker is up."

# Create topic if not exists
kafka-topics --bootstrap-server $BROKER --create --if-not-exists --topic $TOPIC1 --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server $BROKER --create --if-not-exists --topic $TOPIC2 --partitions 3 --replication-factor 1

# Register schema
# Wait for Schema Registry
echo "Waiting for Schema Registry at $SCHEMA_REGISTRY_URL..."
until curl --silent --fail $SCHEMA_REGISTRY_URL/subjects > /dev/null; do
  sleep 2
done

# Ensure schema files exist before attempting registration
if [ ! -f "$SCHEMA_FILE" ]; then
  echo "Schema file $SCHEMA_FILE not found! Aborting registration."
  exit 1
fi
if [ ! -f "$SCHEMA_FILE2" ]; then
  echo "Schema file $SCHEMA_FILE2 not found! Aborting registration."
  exit 1
fi

echo "Schema Registry is up."

# Helper: register schema with retries and verbose output
register_schema() {
  local schema_file="$1"
  local subject="$2"

  echo "Registering schema file '$schema_file' for subject '$subject'"

  # Read schema file into a JSON string (jq will escape correctly)
  payload=$(jq -Rs . < "$schema_file")
  if [ $? -ne 0 ]; then
    echo "Failed to read/escape schema file $schema_file (is jq installed?)."
    return 1
  fi

  # Try a few times to register and output response for debugging
  attempts=0
  max_attempts=5
  while [ $attempts -lt $max_attempts ]; do
    attempts=$((attempts+1))
    echo "Attempt $attempts to register subject $subject..."

    # Use curl and capture both body and HTTP status code
    resp=$(curl --silent --show-error -w "\n%{http_code}" -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data "{\"schema\":$payload}" \
      "$SCHEMA_REGISTRY_URL/subjects/$subject/versions" ) || true

    http_code=$(echo "$resp" | tail -n1)
    body=$(echo "$resp" | sed '$d')

    echo "HTTP $http_code"
    if [ -n "$body" ]; then
      echo "Response body: $body"
    fi

    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
      echo "Successfully registered schema for subject $subject (attempt $attempts)."
      return 0
    fi

    echo "Registration attempt $attempts failed for subject $subject. Retrying in 2s..."
    sleep 2
  done

  echo "Failed to register schema for subject $subject after $max_attempts attempts."
  return 1
}

# Register schemas
register_schema "$SCHEMA_FILE" "$TOPIC1-value" || exit 1
register_schema "$SCHEMA_FILE2" "$TOPIC2-value" || exit 1

echo "Done."
