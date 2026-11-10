
from kafka import KafkaConsumer
import mysql.connector
import json
import joblib
from datetime import datetime
from zomato_db_config import DB_CONFIG


# -------------------------------
# 1. Load trained ML model
# -------------------------------

model = joblib.load(r"C:\kafka\anomaly_model.pkl")

print("Isolation Forest model loaded successfully!")

# -------------------------------
# 2. Connect to MySQL
# -------------------------------

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

print("Connected to MySQL successfully!")

# -------------------------------
# 3. Connect to Kafka
# -------------------------------

consumer = KafkaConsumer(
    "zomato-orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# -------------------------------
# 4. Analytics variables
# -------------------------------

total_orders = 0
total_revenue = 0
restaurant_orders = {}
status_count = {}

print("Zomato Real-Time Stream Processor Started!")
print("Waiting for new orders...\n")

try:
    for message in consumer:

        order = message.value

        order_id = order["order_id"]
        restaurant = order["restaurant"]
        amount = float(order["amount"])
        delivery_time = int(order["delivery_time_minutes"])
        status = order["status"]

        # Convert timestamp
        order_timestamp = datetime.fromisoformat(
            order["timestamp"]
        ).strftime("%Y-%m-%d %H:%M:%S")

        # -------------------------------
        # 5. ML anomaly prediction
        # -------------------------------

        features = [[amount, delivery_time]]

        prediction = model.predict(features)[0]

        anomaly_score = float(
            model.decision_function(features)[0]
        )

        is_anomaly = 1 if prediction == -1 else 0

        anomaly_label = (
            "ANOMALY" if is_anomaly == 1 else "NORMAL"
        )

        # -------------------------------
        # 6. Insert order and ML results
        # -------------------------------

        sql = """
        INSERT INTO orders
        (order_id, restaurant, amount,
         delivery_time_minutes, status, order_timestamp,
         is_anomaly, anomaly_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON DUPLICATE KEY UPDATE
            restaurant = VALUES(restaurant),
            amount = VALUES(amount),
            delivery_time_minutes = VALUES(delivery_time_minutes),
            status = VALUES(status),
            order_timestamp = VALUES(order_timestamp),
            is_anomaly = VALUES(is_anomaly),
            anomaly_score = VALUES(anomaly_score)
        """

        values = (
            order_id,
            restaurant,
            amount,
            delivery_time,
            status,
            order_timestamp,
            is_anomaly,
            anomaly_score
        )

        cursor.execute(sql, values)
        db.commit()

        print("Order saved to MySQL:", order_id)

        # -------------------------------
        # 7. Update live analytics
        # -------------------------------

        total_orders += 1
        total_revenue += amount

        restaurant_orders[restaurant] = (
            restaurant_orders.get(restaurant, 0) + 1
        )

        status_count[status] = (
            status_count.get(status, 0) + 1
        )

        print("=" * 45)
        print("NEW ORDER RECEIVED")
        print("Order ID:", order_id)
        print("Restaurant:", restaurant)
        print("Amount: Rs.", amount)
        print("Delivery Time:", delivery_time, "minutes")
        print("Status:", status)

        print("\nMACHINE LEARNING RESULT")
        print("Prediction:", anomaly_label)
        print("Anomaly Score:", round(anomaly_score, 4))

        print("\nLIVE ANALYTICS")
        print("Total Orders:", total_orders)
        print("Total Revenue: Rs.", round(total_revenue, 2))

        print(
            "Average Order Value: Rs.",
            round(total_revenue / total_orders, 2)
        )

        print("\nRestaurant-wise Orders:")
        for name, count in restaurant_orders.items():
            print(name, ":", count)

        print("\nOrder Status Count:")
        for name, count in status_count.items():
            print(name, ":", count)

except KeyboardInterrupt:
    print("\nStream processing stopped.")

finally:
    consumer.close()
    cursor.close()
    db.close()
    print("Kafka and MySQL connections closed.")
