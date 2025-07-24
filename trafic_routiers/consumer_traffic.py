from kafka import KafkaConsumer
from google.cloud import bigquery
import json

# Config BigQuery
import os

# Spécifie le chemin vers ton fichier de service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-service-account.json"

# Maintenant tu peux instancier le client
client = bigquery.Client()

table_id = "air-pollution-pipeline.pollution_dataset.activity_data"  # adapte ça

# Kafka Consumer config
consumer = KafkaConsumer(
    "activity_data",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='activity-consumer-group'
)

print("🟢 En écoute sur le topic 'activity_data'...")

for message in consumer:
    row = message.value
    print("📥 Reçu :", row)

    # Envoi BigQuery
    errors = client.insert_rows_json(table_id, [row])
    if errors == []:
        print("✅ Inséré avec succès")
    else:
        print("❌ Erreurs :", errors)
