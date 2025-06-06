import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
time.sleep(10) 
# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers = 'kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Danh sách người dùng và hành vi giả lập
user_ids = [f"user_{i}" for i in range(1, 6)]
pages = ["/", "/home", "/product", "/cart", "/checkout"]
actions = ["view", "click", "scroll", "add_to_cart", "purchase"]

# Hàm sinh log
def generate_log():
    return {
        "user_id": random.choice(user_ids),
        "page": random.choice(pages),
        "action": random.choice(actions),
        "timestamp": datetime.utcnow().isoformat()
    }

# Gửi log vào Kafka topic "user_behavior" mỗi giây
if __name__ == "__main__":
    while True:
        log = generate_log()
        producer.send("user_behavior", log)
        print("Sent:", log)
        time.sleep(1)
