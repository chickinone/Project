# Kafka → Flink → PostgreSQL Pipeline

## How to run

```bash
docker-compose up --build
```

## Kafka Topic
- Topic: `laptop_raw_data`

## PostgreSQL
- DB: `laptopdb`
- Table: `laptops(name, brand, cpu, price)`