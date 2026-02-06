#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b "$KAFKA_BOOTSTRAP_SERVERS" 1 60

echo "Creating Kafka topics..."

kafka-topics --create --if-not-exists \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --replication-factor 1 \
  --partitions 3 \
  --topic customer-application

kafka-topics --create --if-not-exists \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --replication-factor 1 \
  --partitions 3 \
  --topic treatment-required

kafka-topics --create --if-not-exists \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --replication-factor 1 \
  --partitions 3 \
  --topic treatment-results

kafka-topics --create --if-not-exists \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --replication-factor 1 \
  --partitions 3 \
  --topic customer-application-response

echo "Topics created successfully:"
kafka-topics --list --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS"
