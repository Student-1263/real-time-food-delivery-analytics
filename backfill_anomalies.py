import mysql.connector
import pandas as pd
import joblib
from zomato_db_config import DB_CONFIG


# Load the already trained model
model = joblib.load(r"C:\kafka\anomaly_model.pkl")

print("Isolation Forest model loaded successfully!")

# Connect to MySQL
db = mysql.connector.connect(**DB_CONFIG)

cursor = db.cursor()

print("Connected to MySQL successfully!")

# Fetch all historical orders
query = """
SELECT order_id, amount, delivery_time_minutes
FROM orders
"""

df = pd.read_sql(query, db)

print("Total orders fetched:", len(df))

# Prepare features
features = df[["amount", "delivery_time_minutes"]]

# Predict anomalies
predictions = model.predict(features)
scores = model.decision_function(features)

# Update MySQL records
update_query = """
UPDATE orders
SET is_anomaly = %s,
    anomaly_score = %s
WHERE order_id = %s
"""

for i, row in df.iterrows():

    is_anomaly = 1 if predictions[i] == -1 else 0
    anomaly_score = float(scores[i])

    cursor.execute(
        update_query,
        (is_anomaly, anomaly_score, int(row["order_id"]))
    )

# Commit changes
db.commit()

# Display results
normal_count = sum(predictions == 1)
anomaly_count = sum(predictions == -1)

print("\nHistorical anomaly detection completed!")
print("Total orders:", len(df))
print("Normal orders:", normal_count)
print("Anomalous orders:", anomaly_count)

cursor.close()
db.close()

print("\nBackfill completed successfully!")