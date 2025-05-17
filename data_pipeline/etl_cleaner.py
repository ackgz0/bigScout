from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, lit, avg, first

# Spark oturumu başlat
spark = SparkSession.builder \
    .appName("Cleaned Player Age Fix for 3 Seasons") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigscout.players") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bigscout.cleaned_players_agg4") \
    .getOrCreate()

# Mongo'dan veriyi oku
df = spark.read.format("mongo").load()

# Sadece 2024-2025 sezonu olan oyuncuları bul (bunun dışındakiler dahil edilmeyecek)
players_with_2025 = df.filter(col("season") == "2024-2025").select("Player").distinct()

# Veriyi bu oyuncularla sınırla
df = df.join(players_with_2025, on="Player", how="inner")

# Yaş bilgisini 38-121 gibi ifadelerden 38 olarak al
df = df.withColumn("Age_base", regexp_extract(col("Age").cast("string"), r'^(\d+)', 1).cast("float"))

# Sezona göre yaş düzeltmeleri
df = df.withColumn("Age", when(col("season") == "2024-2025", col("Age_base"))
                   .when(col("season") == "2023-2024", col("Age_base") - 1)
                   .when(col("season") == "2022-2023", col("Age_base") - 2)
                   .otherwise(None))

# Gerekli kolonlar
group_cols = ["Player", "Nation", "Pos", "Squad", "season", "league"]

metric_cols = [
    "90s", "Ast", "xA", "xAG", "A-xAG", "KP", "PPA", "CrsPA", "PrgP",
    "Cmp", "Att", "Cmp_1", "Cmp_2", "Cmp_3", "TotDist", "PrgDist"
]

# Aggregation: Yaş için "first", diğerleri için "avg"
agg_exprs = [first("Age").alias("Age")] + [avg(c).alias(c) for c in metric_cols]

# Grupla ve hesapla
agg_df = df.groupBy(group_cols).agg(*agg_exprs)

# Göster (kontrol amaçlı)
agg_df.select("Player", "season", "Age", "Ast", "xA", "xAG").show(10)

# MongoDB'ye yaz
agg_df.write.format("mongo").mode("overwrite").save()

spark.stop()
