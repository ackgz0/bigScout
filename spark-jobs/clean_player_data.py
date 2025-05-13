from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# Spark oturumu oluştur
spark = SparkSession.builder \
    .appName("Clean and Join Mongo Player Stats") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigscout.players") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bigscout.cleaned_players") \
    .getOrCreate()

# MongoDB'den 'players' koleksiyonunu oku
df = spark.read.format("mongo").load()

# Mevcut kolonları yazdır
print("Available columns:", df.columns)

# Sayıya çevrilmesi gereken kolonlar (bazıları % veya / içeriyor, adları temizlenecek)
float_columns = [
    "Age", "90s", "Cmp", "Att", "Cmp%", "TotDist", "PrgDist",
    "Cmp_1", "Att_1", "Cmp%_1", "Cmp_2", "Att_2", "Cmp%_2",
    "Cmp_3", "Att_3", "Cmp%_3", "Ast", "xAG", "xA", "A-xAG",
    "KP", "1/3", "PPA", "CrsPA", "PrgP"
]

# Kolonları uygun float tipine dönüştür ("/" ve "%" içerenleri güvenli isimle yeniden adlandır)
for col_name in float_columns:
    if col_name in df.columns:
        safe_name = col_name.replace("/", "_").replace("%", "Pct")
        df = df.withColumn(safe_name, col(col_name).cast(FloatType()))
    else:
        print(f"Column not found in DataFrame: {col_name}")

# Sonuçları göster (örnek olarak bazı kolonları gösteriyoruz)
df.select("Player", "Age", "Ast", "xA").show(10)

# Eğer veri yoksa yazılmaz, kontrol et
if df.count() > 0:
    df.write.format("mongo") \
        .mode("overwrite") \
        .option("database", "bigscout") \
        .option("collection", "cleaned_players") \
        .save()
    print("✅ Data successfully written to 'cleaned_players' collection.")
else:
    print("⚠️ DataFrame is empty. Nothing was written to MongoDB.")

# Spark oturumunu kapat
spark.stop()
