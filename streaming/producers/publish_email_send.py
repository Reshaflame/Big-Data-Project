import sys, csv, json, random, time, os
from kafka import KafkaProducer
from datetime import datetime

csv_path  = sys.argv[1]
sleep_min = float(sys.argv[2])
sleep_max = float(sys.argv[3])
topic     = os.getenv("KAFKA_TOPIC", "email_send_stream")
servers   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
    acks="all"
)

with open(csv_path, newline="", encoding="utf-8") as fh:
    for i, row in enumerate(csv.DictReader(fh), 1):
        print(f"Publishing record: {row}")
        producer.send(topic, row)
        if i % 100 == 0:
            print(f"{datetime.utcnow().isoformat()} sent {i} rows")
        time.sleep(random.uniform(sleep_min, sleep_max))

producer.flush()
print(f"✓ streamed {i} total rows from {csv_path}")
