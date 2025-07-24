import csv, json, time
from kafka import KafkaProducer

CSV_PATH = "merged_activity.csv"
TOPIC    = "activity_data"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def norm_time(t):
    """ '10:22' -> '10:22:00' ; ''/None -> None """
    if not t:
        return None
    t = t.strip()
    return t + ":00" if len(t) == 5 else t

count = 0
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # drop colonnes parasites
        row.pop("Unnamed: 0", None)

        # normaliser types
        row["Start"] = norm_time(row.get("Start"))
        row["End"]   = norm_time(row.get("End"))

        for col in ("Activity", "Day", "user_id"):
            val = row.get(col)
            row[col] = int(val) if val not in (None, "", "NaN") else None

        # envoi
        producer.send(TOPIC, value=row)
        count += 1

producer.flush()
producer.close()
print(f" Producteur terminé : {count} lignes envoyées.")