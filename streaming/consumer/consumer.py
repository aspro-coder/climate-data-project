import json
from kafka import KafkaConsumer

TOPIC = "weather-stream"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:29092",
    auto_offset_reset="earliest",
    group_id="weather-consumer-group",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...", flush=True)

for message in consumer:

    data = message.value

    city = data.get("name")
    temp = data.get("main", {}).get("temp")
    ts = data.get("timestamp")

    print(
        f"Received -> "
        f"city={city}, "
        f"temp={temp}, "
        f"time={ts}",
        flush=True
    )
