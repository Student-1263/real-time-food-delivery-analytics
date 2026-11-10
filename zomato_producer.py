
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

restaurants = ["Spice Garden", "Burger Hub", "Dosa Corner"]
statuses = ["PLACED", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"]

print("Starting Zomato order stream. Press Ctrl+C to stop.")

try:
    order_id = 1001

    while True:
        order = {
            "order_id": order_id,
            "restaurant": random.choice(restaurants),
            "amount": round(random.uniform(100, 800), 2),
            "delivery_time_minutes": random.randint(15, 60),
            "status": random.choice(statuses),
            "timestamp": datetime.now().isoformat()
        }

        producer.send("zomato-orders", value=order)
        producer.flush()

        print("Sent:", order)
        order_id += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\nOrder stream stopped.")

finally:
    producer.close()
