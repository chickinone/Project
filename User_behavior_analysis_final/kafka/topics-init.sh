#!/bin/bash

echo "⏳ Waiting for Kafka to be ready..."
sleep 10

echo "⚙️ Checking and creating topic: user_behavior"

# Chỉ tạo topic nếu chưa tồn tại
kafka-topics --bootstrap-server kafka:9092 --list | grep -q user_behavior || \
kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic user_behavior

echo "✅ Topics created/listed:"
kafka-topics --list --bootstrap-server kafka:9092
