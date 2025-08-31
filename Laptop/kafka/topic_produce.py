import os
import csv
import json
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fact_dir = 'D/Test/Clean_data'
for filename in os.listdir(fact_dir):
    if filename.startswith('fact_') and filename.endswith('.csv'):
        topic_name = filename.replace('.csv', '_topic')  
        filepath = os.path.join(fact_dir, filename)
        print(f"\n🟢 Sending data from {filename} → Kafka topic: {topic_name}")

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                producer.send(topic_name, value=row)
                print(f"Sent to {topic_name}: {row}")
                time.sleep(0.5)  

producer.flush()
producer.close()
print("\n Finished sending all fact data to Kafka.")
