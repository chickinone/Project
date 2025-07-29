from kafka import KafkaConsumer
import psycopg2
import json

# Kết nối PostgreSQL
conn = psycopg2.connect(
    dbname="user_behavior",
    user="user",
    password="password",
    host="postgres",   # 💡 rất quan trọng: dùng đúng tên service trong docker-compose
    port="5432"
)
cur = conn.cursor()

# Kết nối Kafka Consumer
consumer = KafkaConsumer(
    'user_behavior',
    bootstrap_servers='kafka:9092', 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='user-log-consumer-group'
)

print("✅ Consumer started. Listening to 'user_behavior'...")

# Lắng nghe và lưu log vào PostgreSQL
for message in consumer:
    log = message.value
    print("📥 Received:", log)

    cur.execute("""
        INSERT INTO user_logs (user_id, page, action, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (
        log.get("user_id"),
        log.get("page"),
        log.get("action"),
        log.get("timestamp")
    ))
    conn.commit() 