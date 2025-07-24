import pandas as pd
import json
import time
from kafka import KafkaProducer

# Lire les données CSV
df = pd.read_csv("merged_activity.csv")

# Kafka Producer config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoi ligne par ligne dans le topic "activity_data"
for _, row in df.iterrows():
    message = {
        "activity_code": row["Activity"],
        "start": row["Start"],
        "end": row["End"],
        "day": row["Day"],
        "user_id": row["user_id"]
    }
    print("Envoi :", message)
    producer.send("activity_data", value=message)
    time.sleep(1)  # ajuste si besoin

producer.flush()
producer.close()
print("Fin de l’envoi Kafka")
