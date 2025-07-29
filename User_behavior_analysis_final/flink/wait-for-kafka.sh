#!/bin/bash
set -e

host="kafka"
port=9092

echo "⏳ Waiting for Kafka at $host:$port..."

# ✅ Dùng netcat kiểm tra TCP thay vì curl
until nc -z $host $port; do
  echo "⏳ Still waiting for $host:$port..."
  sleep 1
done

echo "✅ Kafka is up!"
python /opt/flink/flink_job.py
