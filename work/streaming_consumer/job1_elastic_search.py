"""
Streaming upit: Real-time analiza temperature u Abu Dhabiju u odnosu
na istorijski klimatski prosek (1990–1999).

Racuna se prosecna temperatura u kliznom prozoru od 60 minuta
i poredi se sa istorijskim mesecnim prosekom za isti mesec kroz
10-godisnji period, kako bi se detektovala odstupanja od normale.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import requests

# Kreira ES index sa eksplicitnim mappingom ako ne postoji.
# Neophodno jer ES automatski mapira timestamp kolone kao 'long' tip,
# sto onemogucava filtriranje po vremenu u Kibani.
def create_es_index():
    url = "http://elasticsearch:9200/realtime_abu_dhabi_temp"
    exists = requests.head(url)
    if exists.status_code == 404:
        mapping = {
            "mappings": {
                "properties": {
                    "city":           {"type": "keyword"},
                    "window_start":   {"type": "date"},
                    "window_end":     {"type": "date"},
                    "hist_year":      {"type": "integer"},
                    "hist_month":     {"type": "integer"},
                    "stream_avg_temp":{"type": "float"},
                    "hist_avg_temp":  {"type": "float"},
                    "abs_deviation":  {"type": "float"},
                    "pct_deviation":  {"type": "float"},
                    "event_count":    {"type": "integer"},
                    "computed_at":    {"type": "date"}
                }
            }
        }
        requests.put(url, json=mapping)
        print(">> ES index kreiran sa mappingom")
    else:
        print(">> ES index vec postoji, preskacam kreiranje")

create_es_index()
spark = SparkSession.builder \
    .appName("AbuDhabiTemperature") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HDFS_PARQUET = "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
PG_URL       = "jdbc:postgresql://postgresql:5432/weather"
PG_TABLE     = "realtime_abu_dhabi_temp"
TARGET_CITY  = "Abu Dhabi"

# Istorijski referentni podaci
# Racuna se mesecni prosek temperature za Abu Dhabi u periodu 1990–1999
# za isti mesec kao trenutni (real-time) stream.
# Ovo predstavlja "normalu" u odnosu na koju se meri odstupanje.
hist_df = spark.read.parquet(HDFS_PARQUET)

current_month = datetime.now().month
hist_ref = (
    hist_df
    .filter(
        (col("city_name") == TARGET_CITY) &
        (col("year").between(1990, 1999)) &
        (col("month") == current_month)
    )
    .groupBy("year", "month")
    .agg(round(avg("temperature_2m"), 3).alias("hist_avg_temp"))
    .orderBy("year")
)

print(f"\n=== Istorijski referentni podaci za {TARGET_CITY} (Jun, 1990-1999) ===")
hist_ref.show(truncate=False)

# Konverzija istorijskih podataka u Python listu — izbegava crossJoin konflikt unutar foreachBatch-a
hist_ref_list = hist_ref.collect()
hist_ref_schema = hist_ref.schema

schema = StructType([
    StructField("name", StringType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
    ]))
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-stream") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
)

weather = parsed.select(
    col("data.name").alias("city"),
    col("kafka_timestamp").alias("event_time"),
    col("data.main.temp").alias("temperature"),
).filter(col("city") == TARGET_CITY)

# Streaming agregacija:
# - watermark sprečava kasne evente starije od 5 minuta
# - 60-minutni sliding window sa pomeranjem od 5 minuta
# - omogucava real-time pracenje temperaturnih trendova
windowed = (
    weather
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "60 minutes", "5 minutes"),
        "city"
    )
    .agg(
        round(avg("temperature"), 3).alias("stream_avg_temp"),
        count("*").alias("event_count")
    )
)

# foreachBatch omogucava kombinovanje streaming i batch logike:
# - streaming agregacija (60-min prozor)
# - join sa istorijskim podacima
# - upis u Elasticsearch po batch-u
def write_to_elasticsearch(batch_df, batch_id):
def write_to_elasticsearch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    spark_session = batch_df.sql_ctx.sparkSession
    hist_spark = spark_session.createDataFrame(hist_ref_list, hist_ref_schema)

    result_base = batch_df.select(
        "city",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "stream_avg_temp",
        "event_count"
    )

    result = (
        result_base
        .crossJoin(hist_spark)
        .withColumn("abs_deviation",
            round(col("stream_avg_temp") - col("hist_avg_temp"), 3))
        .withColumn("pct_deviation",
            round(
                (col("stream_avg_temp") - col("hist_avg_temp"))
                / col("hist_avg_temp") * 100.0,
            3))
        .withColumn("hist_month", col("month"))
        .withColumn("hist_year",  col("year"))
        .withColumn("computed_at", current_timestamp())
        .select(
            "city", "window_start", "window_end",
            "hist_year", "hist_month",
            "stream_avg_temp", "hist_avg_temp",
            "abs_deviation", "pct_deviation",
            "event_count", "computed_at"
        )
        .orderBy("hist_year")
    )

    rows = result.collect()

    print(f"\n{'='*70}")
    print(f"[Job1] Batch {batch_id}  |  {TARGET_CITY} — temperatura vs istorijski prosek (60-min window)")
    print(f"{'='*70}")
    for row in rows:
        sign = "+" if row['abs_deviation'] >= 0 else ""
        print(f"  window          : {row['window_start']} -> {row['window_end']}")
        print(f"  hist_year       : {row['hist_year']}")
        print(f"  stream_avg_temp : {row['stream_avg_temp']} C")
        print(f"  hist_avg_temp   : {row['hist_avg_temp']} C  (Jun {row['hist_year']})")
        print(f"  abs_deviation   : {sign}{row['abs_deviation']} C")
        print(f"  pct_deviation   : {sign}{row['pct_deviation']} %")
        print(f"  event_count     : {row['event_count']}")
        print(f"  computed_at     : {row['computed_at']}")
        print()
 
    # Upisuje rezultate u Elasticsearch
    # crossJoin sa hist_spark daje po jedan red za svaku istorijsku godinu
    # (1990-1999), sto omogucava poredjenje trenutne temperature sa
    # klimatskim odlikama pojedinacnih godina.
    
    result.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "realtime_abu_dhabi_temp") \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.date.rich", "true") \
        .mode("append") \
        .save()
    # Pauza izmedju batcheva da bi ispis u terminalu bio citljiviji
    import time
    time.sleep(3)

query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_elasticsearch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/q1_abu_dhabi_temp") \
    .start()

query.awaitTermination()
