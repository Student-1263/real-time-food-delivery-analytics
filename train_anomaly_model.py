import mysql.connector
import pandas as pd
import joblib

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from zomato_db_config import DB_CONFIG

# --------------------------------
# 1. CONNECT TO MYSQL
# --------------------------------

db = mysql.connector.connect(**DB_CONFIG)

print("Connected to MySQL successfully!")

# --------------------------------
# 2. FETCH HISTORICAL ORDERS
# --------------------------------

query = """
SELECT amount, delivery_time_minutes
FROM orders
"""

df = pd.read_sql(query, db)

db.close()

print("Total historical orders:", len(df))

# --------------------------------
# 3. SELECT ML FEATURES
# --------------------------------

features = df[
    ["amount", "delivery_time_minutes"]
].dropna()

if len(features) < 10:
    raise ValueError("At least 10 valid orders are required.")

# --------------------------------
# 4. TRAIN ISOLATION FOREST
# --------------------------------

model = Pipeline([
    ("scaler", StandardScaler()),
    ("isolation_forest", IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42
    ))
])

model.fit(features)

print("Isolation Forest model trained successfully!")

# --------------------------------
# 5. SAVE TRAINED MODEL
# --------------------------------

joblib.dump(model, "anomaly_model.pkl")

print("Model saved as anomaly_model.pkl")

# --------------------------------
# 6. VERIFY MODEL
# --------------------------------

predictions = model.predict(features)

anomalies = (predictions == -1).sum()
normal = (predictions == 1).sum()

print("Normal orders:", normal)
print("Anomalous orders:", anomalies)

print("Training completed successfully!")