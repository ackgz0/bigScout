# consumer.py
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Consumer tanımı
consumer = KafkaConsumer(
    'player_stats',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# MongoDB bağlantısı
client = MongoClient('localhost', 27017)
db = client.bigscout
collection = db.players

print("⏳ Listening for messages on 'player_stats' topic...")

for message in consumer:
    player = message.value
    print(f"📥 Received: {player['name']}")

    # MongoDB'ye ekle (güncelse güncelle)
    collection.update_one(
        {"name": player["name"]},  # eşleşme kriteri
        {"$set": player},          # veriyi güncelle / ekle
        upsert=True
    )
