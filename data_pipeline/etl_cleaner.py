from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, lit, avg, first

# start a Spark session
spark = SparkSession.builder \
    .appName("Cleaned Player Age Fix for 3 Seasons") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigscout.players") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bigscout.cleaned_players_agg4") \
    .getOrCreate()

# read data from Mongo
df = spark.read.format("mongo").load()

# find players who had valid data in the 2024–2025 season
players_with_2025 = df.filter(col("season") == "2024-2025").select("Player").distinct()

# limit data to these players
df = df.join(players_with_2025, on="Player", how="inner")

# !! normalize the age data !!
df = df.withColumn("Age_base", regexp_extract(col("Age").cast("string"), r'^(\d+)', 1).cast("float"))

# age data corrections according to seasons
df = df.withColumn("Age", when(col("season") == "2024-2025", col("Age_base"))
                   .when(col("season") == "2023-2024", col("Age_base") - 1)
                   .when(col("season") == "2022-2023", col("Age_base") - 2)
                   .otherwise(None))

# necessary columns
group_cols = ["Player", "Nation", "Pos", "Squad", "season", "league"]

metric_cols = [
    "90s", "Ast", "xA", "xAG", "A-xAG", "KP", "PPA", "CrsPA", "PrgP",
    "Cmp", "Att", "Cmp_1", "Cmp_2", "Cmp_3", "TotDist", "PrgDist"
]

# aggregation: "first" for age, "avg" for others
agg_exprs = [first("Age").alias("Age")] + [avg(c).alias(c) for c in metric_cols]

# grouping ve calculation
agg_df = df.groupBy(group_cols).agg(*agg_exprs)

# show for check
agg_df.select("Player", "season", "Age", "Ast", "xA", "xAG").show(10)

# write to Mongo
agg_df.write.format("mongo").mode("overwrite").save()

spark.stop()
