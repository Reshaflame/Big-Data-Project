import os, json, time, sys
import pandas as pd
from kafka import KafkaProducer

csv      = os.environ["CSV_FILE"]
topic    = os.environ["KAFKA_TOPIC"]
servers  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
pause_ms = int(os.environ.get("THROTTLE_MS", "0"))

producer = KafkaProducer(bootstrap_servers=servers,
                         value_serializer=lambda v: json.dumps(v).encode())

df = pd.read_csv(csv, dtype=str)       # keep everything as str → JSON-safe
for _, row in df.iterrows():
    producer.send(topic, row.to_dict())
    if pause_ms:
        time.sleep(pause_ms / 1000)

producer.flush()
print(f"✓ streamed {len(df):,} rows from {csv} to topic '{topic}'")
