from kafka import KafkaProducer
import pandas as pd
import json
import time


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv("D:/Laptop/Clean_data/cleaned_laptop_data.csv") 

for idx, row in df.iterrows():
    message = row.to_dict()
    producer.send("laptop_raw_data", value=message)
    print(f"[{idx}] Sent: {message['name'][:40]}")
    time.sleep(0.1)  
