from pyspark.sql import SparkSession

# SparkSession başlat
spark = SparkSession.builder \
    .appName("MongoDB Reader") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigscout.players") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bigscout.players") \
    .getOrCreate()

# MongoDB'den veriyi oku
df = spark.read.format("mongo").load()

# Schema'yı göster
df.printSchema()

# İlk 10 kaydı göster
df.show(10, truncate=False)

# Gerekirse sezon filtresi uygula
filtered = df.filter(df["season"] == "2023-2024")
filtered.show(10, truncate=False)

# Spark kapat
spark.stop()
