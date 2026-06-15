"""
STREAMING UPIT 4: Detekcija naglih promena osobina vetra u Cikagu

Ovaj streaming job obrađuje real-time meteoroloske podatke iz Kafka stream-a
i analizira promene vetra u poslednjih 30 minuta.

Cilj:
- detekcija naglog porasta brzine vetra (>50%)
- detekcija jakih udara vetra (>30% iznad proseka)
- detekcija značajne promene pravca vetra (>45°)
- generisanje kombinovanog "wind_alert" signala
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# Kreira ES index sa eksplicitnim mappingom ako ne postoji.
# Neophodno jer ES automatski mapira timestamp kolone kao 'long' tip,
# sto onemogucava filtriranje po vremenu u Kibani.
# Ako index vec postoji, preskace kreiranje i nastavlja normalno.
def create_es_index():
    url = "http://elasticsearch:9200/realtime_chicago_wind"
    exists = requests.head(url)
    if exists.status_code == 404:
        mapping = {
            "mappings": {
                "properties": {
                    "city":                 {"type": "keyword"},
                    "window_start":         {"type": "date"},
                    "window_end":           {"type": "date"},
                    "avg_wind_speed":       {"type": "float"},
                    "min_wind_speed":       {"type": "float"},
                    "max_wind_speed":       {"type": "float"},
                    "wind_speed_rise_pct":  {"type": "float"},
                    "avg_gust":             {"type": "float"},
                    "max_gust":             {"type": "float"},
                    "gust_above_avg_pct":   {"type": "float"},
                    "avg_wind_dir":         {"type": "float"},
                    "wind_dir_change":      {"type": "float"},
                    "speed_alert":          {"type": "boolean"},
                    "gust_alert":           {"type": "boolean"},
                    "direction_alert":      {"type": "boolean"},
                    "wind_alert":           {"type": "boolean"},
                    "computed_at":          {"type": "date"}
                }
            }
        }
        requests.put(url, json=mapping)
        print(">> ES index kreiran sa mappingom")
    else:
        print(">> ES index vec postoji, preskacam kreiranje")

create_es_index()

spark = SparkSession.builder \
    .appName("ChicagoWindAlert") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("name", StringType()),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg",   DoubleType()),
        StructField("gust",  DoubleType()),
    ]))
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
)

weather = parsed.select(
    col("data.name").alias("city"),
    col("kafka_timestamp").alias("event_time"),
    col("data.wind.speed").alias("wind_speed"),
    col("data.wind.deg").alias("wind_deg"),
    col("data.wind.gust").alias("wind_gust"),
).filter(col("city") == "Chicago")

# Agregacija i racunanje promena u okviru 30-min prozora
windowed = (
    weather
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "30 minutes", "5 minutes"),
        "city"
    )
    .agg(
        round(avg("wind_speed"), 2).alias("avg_wind_speed"),
        round(min("wind_speed"), 2).alias("min_wind_speed"),
        round(max("wind_speed"), 2).alias("max_wind_speed"),
        round(avg("wind_gust"),  2).alias("avg_gust"),
        round(max("wind_gust"),  2).alias("max_gust"),
        round(avg("wind_deg"),   2).alias("avg_wind_dir"),
        round(min("wind_deg"),   2).alias("min_wind_dir"),
        round(max("wind_deg"),   2).alias("max_wind_dir"),
    )
    # % porast brzine vetra (min -> max) unutar prozora
    .withColumn("wind_speed_rise_pct",
        round(
            when(col("min_wind_speed") > 0,
                (col("max_wind_speed") - col("min_wind_speed"))
                / col("min_wind_speed") * 100.0
            ).otherwise(lit(None)),
        2))
    # % koliko je max_gust iznad avg_wind_speed
    .withColumn("gust_above_avg_pct",
        round(
            when(col("avg_wind_speed") > 0,
                (col("max_gust") - col("avg_wind_speed"))
                / col("avg_wind_speed") * 100.0
            ).otherwise(lit(None)),
        2))
    # kruzna razlika smera vetra (min -> max)
    .withColumn("raw_diff",
        abs(col("max_wind_dir") - col("min_wind_dir")) % 360.0)
    .withColumn("wind_dir_change",
        round(when(col("raw_diff") > 180.0, lit(360.0) - col("raw_diff"))
              .otherwise(col("raw_diff")), 2))
    .drop("raw_diff")
    # alarmni uslovi
    .withColumn("speed_alert",     col("wind_speed_rise_pct") > 50.0)
    .withColumn("gust_alert",      col("gust_above_avg_pct")  > 30.0)
    .withColumn("direction_alert", col("wind_dir_change")     > 45.0)
    # kombinovani alarm — bilo koji uslov
    .withColumn("wind_alert",
        col("speed_alert") | col("gust_alert") | col("direction_alert"))
)

def write_to_elasticsearch(batch_df, batch_id):
    final_df = batch_df.select(
        "city",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_wind_speed", "min_wind_speed", "max_wind_speed",
        "wind_speed_rise_pct",
        "avg_gust", "max_gust", "gust_above_avg_pct",
        "avg_wind_dir", "wind_dir_change",
        "speed_alert", "gust_alert", "direction_alert", "wind_alert",
        current_timestamp().alias("computed_at")
    )

    rows = final_df.collect()

    print(f"\n{'='*70}")
    print(f"[Job4] Batch {batch_id}  |  Chicago — nagle promene vetra (30-min window)")
    print(f"{'='*70}")
    for row in rows:
        print(f"  window             : {row['window_start']} -> {row['window_end']}")
        print(f"  avg_wind_speed     : {row['avg_wind_speed']} m/s")
        print(f"  wind_speed_rise    : {row['wind_speed_rise_pct']} %")
        print(f"  max_gust           : {row['max_gust']} m/s")
        print(f"  gust_above_avg     : {row['gust_above_avg_pct']} %")
        print(f"  wind_dir_change    : {row['wind_dir_change']} stepeni")
        print(f"  speed_alert        : {row['speed_alert']}")
        print(f"  gust_alert         : {row['gust_alert']}")
        print(f"  direction_alert    : {row['direction_alert']}")
        print(f"  WIND ALERT         : {row['wind_alert']}")
        print(f"  computed_at        : {row['computed_at']}")
        print()

    if rows:
        # Upisuje rezultate proracuna u Elasticsearch
        # Koristi append mod — svaki proracun se cuva kao novi dokument,
        # sto omogucava pracenje istorije promena vrednosti kroz vreme.
        final_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "realtime_chicago_wind") \
            .option("es.nodes.wan.only", "true") \
            .option("es.mapping.date.rich", "true") \
            .mode("append") \
            .save()

query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_elasticsearch) \
    .option("checkpointLocation", "/tmp/checkpoint/q4_chicago_wind") \
    .start()

query.awaitTermination()
