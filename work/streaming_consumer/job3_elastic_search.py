"""
STREAMING UPIT 3: Detekcija rizika od toplotnog udara (Dubai)

Ovaj streaming job obradjuje real-time meteoroloske podatke iz Kafka stream-a
i analizira promene vlaznosti i temperature u poslednjih 30 minuta.

Na osnovu naglih promena vlaznosti i visokih temperatura generise
indikator rizika od toplotnog udara (risk_level).

Izlaz se upisuje u Elasticsearch u realnom vremenu.
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
    url = "http://elasticsearch:9200/realtime_dubai_heat_risk"
    exists = requests.head(url)
    if exists.status_code == 404:
        mapping = {
            "mappings": {
                "properties": {
                    "city":          {"type": "keyword"},
                    "window_start":  {"type": "date"},
                    "window_end":    {"type": "date"},
                    "min_humidity":  {"type": "float"},
                    "max_humidity":  {"type": "float"},
                    "avg_humidity":  {"type": "float"},
                    "humidity_jump": {"type": "float"},
                    "avg_temp":      {"type": "float"},
                    "max_temp":      {"type": "float"},
                    "risk_level":    {"type": "keyword"}
                }
            }
        }
        requests.put(url, json=mapping)
        print(">> ES index kreiran sa mappingom")
    else:
        print(">> ES index vec postoji, preskacam kreiranje")

create_es_index()




spark = SparkSession.builder \
    .appName("DubaiHumidityRisk") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([

    StructField("name", StringType()),

    StructField("timestamp", StringType()),

    StructField(
        "main",
        StructType([

            StructField(
                "temp",
                DoubleType()
            ),

            StructField(
                "humidity",
                IntegerType()
            )

        ])
    )
])

raw = spark.readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    ) \
    .option(
        "subscribe",
        "weather-stream"
    ) \
    .option(
        "startingOffsets",
        "latest"
    ) \
    .load()

parsed = raw.select(

    from_json(

        col("value").cast("string"),

        schema

    ).alias("data")

)

weather = parsed.select(

    col("data.name").alias(
        "city"
    ),

    to_timestamp(

        col("data.timestamp"),

        "yyyy-MM-dd HH:mm:ss"

    ).alias(
        "event_time"
    ),

    col("data.main.temp").alias(
        "temperature"
    ),

    col("data.main.humidity").alias(
        "humidity"
    )

).filter(

    col("city") == "Dubai"

)
# 30-min sliding window sa update-om svakih 5 minuta
# watermark (30 min) uklanja zakasnele dogadjaje
# agregacija ekstremne vrednosti vlage i temperature u realnom vremenu
risk = weather \
.withWatermark(
    "event_time",
    "30 minutes"
) \
.groupBy(

    window(

        col("event_time"),

        "30 minutes",

        "5 minutes"

    ),

    "city"

).agg(

    min(
        "humidity"
    ).alias(
        "min_humidity"
    ),

    max(
        "humidity"
    ).alias(
        "max_humidity"
    ),

    avg(
        "humidity"
    ).alias(
        "avg_humidity"
    ),

    avg(
        "temperature"
    ).alias(
        "avg_temp"
    ),

    max(
        "temperature"
    ).alias(
        "max_temp"
    )

).withColumn(

    "humidity_jump",

    round(

        col(
            "max_humidity"
        )

        -

        col(
            "min_humidity"
        ),

        2

    )

# Izracunavanje heat-risk logike na osnovu:
# - naglog skoka vlaznosti
# - ekstremno visoke vlaznosti uz visoku temperaturu
# - kombinovanih pragova za HIGH / CRITICAL stanje
).withColumn(

    "risk_level",

    when(

        (

            col(
                "humidity_jump"
            ) > 20

        )

        |

        (

            (col(
                "max_humidity"
            ) > 85)

            &

            (col(
                "avg_temp"
            ) > 35)

        ),

        "CRITICAL"

    ).when(

        (

            col(
                "humidity_jump"
            ) > 12

        )

        |

        (

            (col(
                "max_humidity"
            ) > 75)

            &

            (col(
                "avg_temp"
            ) > 33)

        ),

        "HIGH"

    ).when(

        col(
            "humidity_jump"
        ) > 8,

        "MEDIUM"

    ).otherwise(

        "LOW"

    )

)
# foreachBatch omogucava obradu streaming batch-eva:
# - selektuje window rezultate
# - ispisuje real-time alert u konzoli
# - upisuje rezultate u Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):

    print(
        f"Processing batch {batch_id}"
    )

    final_df = batch_df.select(

        "city",

        col(
            "window.start"
        ).alias(
            "window_start"
        ),

        col(
            "window.end"
        ).alias(
            "window_end"
        ),

        "min_humidity",

        "max_humidity",

        "avg_humidity",

        "humidity_jump",

        "avg_temp",

        "max_temp",

        "risk_level"

    )
    
    rows = final_df.collect()
    print(f"\n{'='*70}")
    print(f"[Job3] Batch {batch_id}  |  Dubai — rizik od toplotnog udara")
    print(f"{'='*70}")
    for row in rows:
        print(f"  window        : {row['window_start']} -> {row['window_end']}")
        print(f"  avg_humidity  : {row['avg_humidity']:.2f} %")
        print(f"  humidity_jump : {row['humidity_jump']} pp")
        print(f"  avg_temp      : {row['avg_temp']:.2f} C")
        print(f"  max_temp      : {row['max_temp']} C")
        print(f"  RISK LEVEL    : {row['risk_level']}")
        print()
    if rows:
        final_df.write \
          .format(
        "org.elasticsearch.spark.sql"
    ) \
    .option(
        "es.nodes", "elasticsearch"
    ) \
    .option(
        "es.port", "9200"
    ) \
    .option(
        "es.resource", "realtime_dubai_heat_risk"
    ) \
    .option(
        "es.nodes.wan.only", "true"
    ) \
    .option("es.mapping.date.rich", "true"
    ) \
    .mode(
        "append"
    ) \
    .save()

query = risk.writeStream \
.outputMode(
    "update"
) \
.foreachBatch(
    write_to_elasticsearch
) \
.start()

query.awaitTermination()

