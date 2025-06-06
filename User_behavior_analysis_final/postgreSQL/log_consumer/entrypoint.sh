#!/bin/bash
set -e

host="postgres"
port=5432
user="user"

echo "⏳ Waiting for PostgreSQL at $host:$port..."

# Đợi PostgreSQL sẵn sàng
until pg_isready -h "$host" -p "$port" -U "$user" > /dev/null 2>&1; do
  sleep 1
done

echo "✅ PostgreSQL is ready!"
sleep 3

exec python log_consumer.py
