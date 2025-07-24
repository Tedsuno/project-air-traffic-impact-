from kafka import KafkaConsumer
import json, os
from google.cloud import bigquery

TOPIC = "activity_data"
TABLE_ID = "air-pollution-pipeline.pollution_dataset.activity"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
bq = bigquery.Client()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="activity-consumer-v1",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def clean(row):
    # Cast types pour coller au schéma BQ
    def as_int(x):
        try: return int(x) if x not in ("", None) else None
        except: return None
    def as_time(x):   # “HH:MM” -> “HH:MM:00”
        if not x: return None
        if len(x) == 5: return x + ":00"
        return x

    return {
        "Activity": as_int(row.get("Activity")),
        "Start":    as_time(row.get("Start")),
        "End":      as_time(row.get("End")),
        "Day":      as_int(row.get("Day")),
        "user_id":  as_int(row.get("user_id")),
    }

print("Consommateur Kafka vers big-query lancé")

for msg in consumer:
    line = clean(msg.value)
    errors = bq.insert_rows_json(TABLE_ID, [line])
    if errors:
        print("erreur d'insertion")
    else:
        print(line)