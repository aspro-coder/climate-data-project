import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC = "weather-stream"
FILE_PATH = "/app/api_data/api_data.json"

producer = None
print("Waiting for Kafka...")
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka")
        break
    except NoBrokersAvailable:
        print(f"Kafka not ready ({i+1}/10)")
        time.sleep(5)

if producer is None:
    raise Exception("Kafka unavailable")

print("Starting stream simulation...")
while True:
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            data["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            producer.send(TOPIC, value=data)
            city = data.get("name")
            temp = data.get("main", {}).get("temp")
            ts = data.get("timestamp")
            print(
                f"Sent -> city={city}, "
                f"temp={temp}, "
                f"time={ts}"
            )
            time.sleep(1)
    print("Reached end of file -> restarting stream")
