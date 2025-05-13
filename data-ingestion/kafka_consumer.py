from kafka import KafkaConsumer
import json

# Kafka consumer oluştur
consumer = KafkaConsumer(
    'player_stats',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # en baştan itibaren oku
    enable_auto_commit=True,
    group_id='bigscout-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🔍 Dinleniyor: player_stats topic")

for message in consumer:
    print(f"[{message.offset}] {message.value}")
