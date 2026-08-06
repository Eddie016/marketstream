#!/bin/sh
set -eu

KAFKA=/opt/kafka/bin/kafka-topics.sh

"$KAFKA" --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic market-prices-v1 --partitions 4 --replication-factor 1
"$KAFKA" --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic market-prices-dlq-v1 --partitions 4 --replication-factor 1
"$KAFKA" --bootstrap-server kafka:9092 --describe --topic market-prices-v1
"$KAFKA" --bootstrap-server kafka:9092 --describe --topic market-prices-dlq-v1
