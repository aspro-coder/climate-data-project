"""
STREAMING UPIT 2: Detekcija rizika od urbanih poplava (Bangkok)

Ovaj streaming job obradjuje real-time meteoroloske podatke iz Kafka stream-a
i poredi trenutne uslove sa istorijskim klimatskim prosekom (1990–1999)
za isti mesec.

Na osnovu odstupanja u oblacnosti i pritisku generise indikator
moguceg rizika od urbanih poplava (flood_risk).

Izlaz se upisuje u Elasticsearch u realnom vremenu.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from datetime import datetime

# Kreira ES index sa eksplicitnim mappingom ako ne postoji.
# Neophodno jer ES automatski mapira timestamp kolone kao 'long' tip,
# sto onemogucava filtriranje po vremenu u Kibani.
def create_es_index():
    url = "http://elasticsearch:9200/realtime_bangkok_flood_risk"
    exists = requests.head(url)
    if exists.status_code == 404:
        mapping = {
            "mappings": {
                "properties": {
                    "city":                      {"type": "keyword"},
                    "window_start":              {"type": "date"},
                    "window_end":                {"type": "date"},
                    "hist_year":                 {"type": "integer"},
                    "hist_month":                {"type": "integer"},
                    "stream_avg_cloud":          {"type": "float"},
                    "hist_avg_cloud":            {"type": "float"},
                    "cloud_anomaly_pct":         {"type": "float"},
                    "stream_avg_pressure":       {"type": "float"},
                    "stream_min_pressure":       {"type": "float"},
                    "stream_max_pressure":       {"type": "float"},
                    "hist_avg_pressure":         {"type": "float"},
                    "pressure_drop_hpa":         {"type": "float"},
                    "pressure_below_hist":       {"type": "float"},
                    "high_cloud_anomaly":        {"type": "boolean"},
                    "pressure_drop_alert":       {"type": "boolean"},
                    "pressure_below_hist_alert": {"type": "boolean"},
                    "flood_risk":                {"type": "boolean"},
                    "event_count":               {"type": "integer"},
                    "computed_at":               {"type": "date"}
                }
            }
        }
        requests.put(url, json=mapping)
        print(">> ES index kreiran sa mappingom")
    else:
        print(">> ES index vec postoji, preskacam kreiranje")

create_es_index()

spark = SparkSession.builder \
    .appName("BangkokFloodRisk") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HDFS_PARQUET = "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
PG_URL       = "jdbc:postgresql://postgresql:5432/weather"
PG_TABLE     = "realtime_bangkok_flood_risk"
TARGET_CITY  = "Bangkok"

hist_df = spark.read.parquet(HDFS_PARQUET)

# Dinamicki odredjuje trenutni mesec umesto fiksnog odredjivanja meseca,
# da bi upit radio ispravno bez obzira kada se pokrene tokom godine
current_month = datetime.now().month
hist_ref = (
    hist_df
    .filter(
        (col("city_name") == TARGET_CITY) &
        (col("year").between(1990, 1999)) &
        (col("month") == current_month)
    )
    .groupBy("year", "month")
    .agg(
        round(avg("cloud_cover"),  3).alias("hist_avg_cloud"),
        round(avg("pressure_msl"), 3).alias("hist_avg_pressure")
    )
    .orderBy("year")
)

print(f"\n=== Istorijski referentni podaci za {TARGET_CITY} (Jun, 1990-1999) ===")
hist_ref.show(truncate=False)

hist_ref_list   = hist_ref.collect()
hist_ref_schema = hist_ref.schema

schema = StructType([
    StructField("name", StringType()),
    StructField("main", StructType([
        StructField("pressure", DoubleType()),
    ])),
    StructField("clouds", StructType([
        StructField("all", DoubleType()),
    ]))
])

# Streaming ingest iz Kafka topic-a (real-time weather podaci)
# maxOffsetsPerTrigger ogranicava kolicinu podataka po batch-u
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-stream") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

# Parsiranje Kafka JSON poruke u strukturirani format
# izdvajamo grad, vreme i meteoroloske vrednosti
parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
)

weather = parsed.select(
    col("data.name").alias("city"),
    col("kafka_timestamp").alias("event_time"),
    col("data.main.pressure").alias("pressure"),
    col("data.clouds.all").alias("cloud_cover"),
).filter(col("city") == TARGET_CITY)

# 90-min sliding window sa update-om svakih 5 minuta
# watermark (5 min) sprecava obradu zakasnelih dogadjaja
# agregiraju se prosecne vrednosti u realnom vremenu
windowed = (
    weather
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "90 minutes", "5 minutes"),
        "city"
    )
    .agg(
        round(avg("cloud_cover"),  3).alias("stream_avg_cloud"),
        round(avg("pressure"),     3).alias("stream_avg_pressure"),
        round(min("pressure"),     3).alias("stream_min_pressure"),
        round(max("pressure"),     3).alias("stream_max_pressure"),
        count("*").alias("event_count")
    )
)

# foreachBatch omogucava kombinovanje streaming i batch obrade:
# - streaming agregati iz window-a
# - join sa istorijskim klimatskim podacima (1990–1999)
# - izracunavanje odstupanja i flood_risk indikatora
# - upis rezultata u Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    spark_session = batch_df.sql_ctx.sparkSession
    hist_spark = spark_session.createDataFrame(hist_ref_list, hist_ref_schema)

    result_base = batch_df.select(
        "city",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "stream_avg_cloud",
        "stream_avg_pressure",
        "stream_min_pressure",
        "stream_max_pressure",
        "event_count"
    )
# Izracunavanje anomalija u odnosu na istorijski prosek
# i generisanje boolean flood_risk signala
    result = (
        result_base
        .crossJoin(hist_spark)
        .withColumn("cloud_anomaly_pct",
            round(
                (col("stream_avg_cloud") - col("hist_avg_cloud"))
                / when(col("hist_avg_cloud") != 0, col("hist_avg_cloud"))
                  .otherwise(lit(1.0)) * 100.0,
            3))
        .withColumn("pressure_drop_hpa",
            round(col("stream_max_pressure") - col("stream_min_pressure"), 3))
        .withColumn("pressure_below_hist",
            round(col("hist_avg_pressure") - col("stream_avg_pressure"), 3))
        .withColumn("high_cloud_anomaly",
            col("cloud_anomaly_pct") > 30.0)
        .withColumn("pressure_drop_alert",
            col("pressure_drop_hpa") > 3.0)
        .withColumn("pressure_below_hist_alert",
            col("pressure_below_hist") >= 2.0)
        .withColumn("flood_risk",
            col("high_cloud_anomaly") &
            col("pressure_drop_alert") &
            col("pressure_below_hist_alert"))
        .withColumn("hist_month", col("month"))
        .withColumn("hist_year",  col("year"))
        .withColumn("computed_at", current_timestamp())
        .select(
            "city", "window_start", "window_end",
            "hist_year", "hist_month",
            "stream_avg_cloud", "hist_avg_cloud", "cloud_anomaly_pct",
            "stream_avg_pressure", "stream_min_pressure", "stream_max_pressure",
            "hist_avg_pressure", "pressure_drop_hpa", "pressure_below_hist",
            "high_cloud_anomaly", "pressure_drop_alert",
            "pressure_below_hist_alert", "flood_risk",
            "event_count", "computed_at"
        )
    )

    rows = result.collect()

    print(f"\n{'='*70}")
    print(f"[Job2] Batch {batch_id}  |  {TARGET_CITY} — rizik od poplava (90-min window)")
    print(f"{'='*70}")
    for row in rows:
        print(f"  window                   : {row['window_start']} -> {row['window_end']}")
        print(f"  hist_year                : {row['hist_year']}")
        print(f"  stream_avg_cloud         : {row['stream_avg_cloud']} %")
        print(f"  hist_avg_cloud           : {row['hist_avg_cloud']} %  (Jun {row['hist_year']})")
        print(f"  cloud_anomaly_pct        : {row['cloud_anomaly_pct']} %")
        print(f"  stream_avg_pressure      : {row['stream_avg_pressure']} hPa")
        print(f"  hist_avg_pressure        : {row['hist_avg_pressure']} hPa  (Jun {row['hist_year']})")
        print(f"  pressure_drop_hpa        : {row['pressure_drop_hpa']} hPa")
        print(f"  pressure_below_hist      : {row['pressure_below_hist']} hPa")
        print(f"  high_cloud_anomaly       : {row['high_cloud_anomaly']}")
        print(f"  pressure_drop_alert      : {row['pressure_drop_alert']}")
        print(f"  pressure_below_hist_alert: {row['pressure_below_hist_alert']}")
        print(f"  FLOOD RISK               : {row['flood_risk']}")
        print(f"  event_count              : {row['event_count']}")
        print(f"  computed_at              : {row['computed_at']}")
        print()

    # Upisuje rezultate u Elasticsearch
    # crossJoin sa hist_spark daje po jedan red za svaku istorijsku godinu
    # (1990-1999), sto omogucava poredjenje trenutnih meteoroloskih uslova
    # sa klimatskim odlikama pojedinacnih godina za detekciju rizika od poplava
    result.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "realtime_bangkok_flood_risk") \
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
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/q2_bangkok_flood") \
    .start()

query.awaitTermination()
