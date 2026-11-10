

\# 🍔 Zomato Real-Time Food Delivery Analytics



A real-time food delivery analytics system built using Apache Kafka, Python, Machine Learning, MySQL and Streamlit.



The project simulates food delivery orders, processes incoming events in real time, detects unusual orders using Isolation Forest, stores results in MySQL and displays business insights through an interactive dashboard.



\## 📌 Project Overview



This project demonstrates how a real-time analytics pipeline can monitor food delivery operations and identify statistically unusual orders.



\### Key Features



\- Real-time simulated food order generation.

\- Event streaming using Apache Kafka.

\- Kafka topic with three partitions.

\- Python-based stream processing.

\- Machine learning-based anomaly detection using Isolation Forest.

\- MySQL database integration.

\- Interactive Streamlit dashboard.

\- Automatic dashboard refresh.

\- Business performance monitoring and unusual-order analysis.



\## 🏗️ System Architecture



```text

Python Order Producer

&#x20;       |

&#x20;       v

&#x20;  Apache Kafka

&#x20;  (3 Partitions)

&#x20;       |

&#x20;       v

&#x20;Python Stream Processor

&#x20;       |

&#x20;       v

&#x20;Isolation Forest Model

&#x20;       |

&#x20;       v

&#x20;     MySQL

&#x20;       |

&#x20;       v

&#x20;Streamlit Dashboard

```



\## 🛠️ Technologies Used



| Technology | Purpose |

|---|---|

| Python | Application development |

| Apache Kafka | Real-time event streaming |

| Scikit-learn | Machine learning |

| Isolation Forest | Anomaly detection |

| MySQL | Data storage |

| Streamlit | Interactive dashboard |

| Pandas and NumPy | Data processing |

| Joblib | Model persistence |



\## 🤖 Machine Learning



The project uses the Isolation Forest algorithm to identify unusual food orders based on:



\- Order amount

\- Delivery time



The model is trained using historical order data and saved for reuse.



The stream processor loads the trained model and predicts whether incoming orders are normal or anomalous.



\*\*Note:\*\* Anomalies represent statistically unusual observations and do not necessarily indicate fraud.



\## 📊 Dashboard



The Streamlit dashboard provides:



\- Total orders

\- Total revenue

\- Average order value

\- Average delivery time

\- Restaurant performance

\- Order status distribution

\- Delivery time analysis

\- Normal versus unusual order distribution

\- Latest orders

\- Unusual orders table





\## 📸 Project Screenshots



\### 1. Kafka Topic – Three Partitions

!\[Kafka Topic](screenshots/kafka-topic.png)



\### 2. Real-Time Order Producer

!\[Producer](screenshots/producer.png)



\### 3. Stream Processing and ML Predictions

!\[Stream Processor](screenshots/stream-processor.png)



\### 4. MySQL Database

!\[MySQL Database](screenshots/mysql-database.png)



\### 5. Streamlit Dashboard

!\[Dashboard Overview](screenshots/dashboard-overview.png)



\### 6. Machine Learning Anomaly Detection

!\[Anomaly Detection](screenshots/anomaly-detection.png)





\## 📁 Project Structure



```text

real-time-food-delivery-analytics/

│

├── zomato\_producer.py

├── zomato\_stream\_processor.py

├── zomato\_dashboard.py

├── train\_anomaly\_model.py

├── backfill\_anomalies.py

├── zomato\_db\_config.py

├── anomaly\_model.pkl

├── requirements.txt

├── .gitignore

├── README.md

│

└── screenshots/

```



\## ⚙️ Prerequisites



\- Python 3.10 or later

\- Apache Kafka

\- Java (compatible with your Kafka installation)

\- MySQL Server

\- pip



\## 🚀 Installation and Setup



\### 1. Clone the repository



```bash

git clone https://github.com/Student-1263/real-time-food-delivery-analytics.git

cd real-time-food-delivery-analytics

```



\### 2. Install dependencies



```bash

pip install -r requirements.txt

```



\### 3. Configure MySQL



Create the database:



```sql

CREATE DATABASE zomato\_analytics;

USE zomato\_analytics;

```



Create the orders table:



```sql

CREATE TABLE orders (

&#x20;   order\_id INT PRIMARY KEY,

&#x20;   restaurant VARCHAR(100),

&#x20;   amount DECIMAL(10,2),

&#x20;   delivery\_time\_minutes INT,

&#x20;   status VARCHAR(50),

&#x20;   order\_timestamp DATETIME,

&#x20;   is\_anomaly TINYINT DEFAULT NULL,

&#x20;   anomaly\_score FLOAT DEFAULT NULL

);

```



\### 4. Configure database credentials



Set the following Windows environment variables:



\- `ZOMATO\_DB\_HOST`

\- `ZOMATO\_DB\_USER`

\- `ZOMATO\_DB\_PASSWORD`

\- `ZOMATO\_DB\_NAME`



Default host and database values are provided in `zomato\_db\_config.py`.



Never commit actual database passwords or credentials.



\### 5. Start Apache Kafka



Start ZooKeeper and the Kafka broker using your Kafka installation's configuration and startup scripts.



Create the Kafka topic:



```bash

kafka-topics.bat --create --topic zomato-orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

```



\### 6. Train the anomaly detection model



```bash

python train\_anomaly\_model.py

```



This generates the saved model file.



\### 7. Start the stream processor



```bash

python zomato\_stream\_processor.py

```



\### 8. Start the order producer



```bash

python zomato\_producer.py

```



\### 9. Launch the dashboard



```bash

python -m streamlit run zomato\_dashboard.py

```



Open:



http://localhost:8501



Run the producer, processor and dashboard in separate terminals. Start the processor before the producer.



\## 📈 Results



The dashboard presents live business metrics calculated from the simulated order stream, including:



\- Order volume and revenue.

\- Average delivery time.

\- Restaurant-level performance.

\- Machine learning-based unusual-order detection.



Displayed values change as new orders enter the pipeline.



\## 🔮 Future Enhancements



\- Integrate a real food delivery API.

\- Add advanced delivery-time prediction.

\- Improve anomaly detection with additional features.

\- Deploy the dashboard to a cloud platform.

\- Add automated alerts for unusual orders.



\## 👩‍💻 Author



\*\*Archana TA\*\*



B.Tech – Computer Science and Engineering (Artificial Intelligence and Machine Learning)



Jain (Deemed-to-be University), Bengaluru



\[LinkedIn](https://linkedin.com/in/archana-ta-7776762a4/) | \[GitHub](https://github.com/Student-1263)



\---



\*Academic project using simulated food delivery data. This is not connected to the actual Zomato platform.\*



