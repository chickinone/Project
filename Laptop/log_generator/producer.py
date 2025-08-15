import os
import time
import pandas as pd
import json
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'  # ✅ sửa lại đúng tên container Kafka
CSV_FOLDER = './data'
SLEEP_TIME = 0

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_csv_to_kafka(file_path, topic_name):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic_name, value=message)
        print(f"[{topic_name}] Sent: {message}")
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    for file in os.listdir(CSV_FOLDER):
        if file.endswith('.csv'):
            topic = file.replace('.csv', '') + "_topic"
            path = os.path.join(CSV_FOLDER, file)
            print(f"🔄 Sending {file} to Kafka topic: {topic}")
            send_csv_to_kafka(path, topic)
    print("All files sent.")
