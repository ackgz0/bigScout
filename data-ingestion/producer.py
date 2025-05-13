# producer.py
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dummy futbolcu verisi
players = [
    {"name": "John Smith", "age": 19, "position": "ST", "matches": 12, "goals": 7, "assists": 3},
    {"name": "Ali Yılmaz", "age": 18, "position": "CM", "matches": 14, "goals": 2, "assists": 5},
    {"name": "Carlos Ruiz", "age": 20, "position": "CB", "matches": 15, "goals": 1, "assists": 0},
]

while True:
    for player in players:
        print(f"Sending: {player['name']}")
        producer.send('player_stats', player)
        time.sleep(2)  # her oyuncuyu 2 saniyede bir gönder
