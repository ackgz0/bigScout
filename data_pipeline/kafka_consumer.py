from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# MongoDB bağlantısı
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["bigscout"]
collection = db["players"]

# Kafka consumer
consumer = KafkaConsumer(
    'player_stats',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ Dinleme başlatıldı. Kafka → MongoDB")

for message in consumer:
    data = message.value
    collection.insert_one(data)
    print("📝 MongoDB'ye kaydedildi:", data['name'] if 'name' in data else data)
