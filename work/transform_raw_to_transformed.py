from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("WeatherDataTransformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# Povecavam broj shuffle particija na 16 da smanjim opterecenje memorije
# tokom "wide" operacija (groupBy, join) i da poboljsam paralelizaciju
# za dataset od oko 2GB.
# Default vrednost (200) je prevelika za mali Docker Spark klaster.
spark.conf.set("spark.sql.shuffle.partitions", "16")

RAW_PATH = "hdfs://namenode:9000/data/raw_zone/hourly_data_combined_1990_to_1999.csv"

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_PATH)
)

print("=== RAW SCHEMA ===")
df_raw.printSchema()

print("=== RAW SAMPLE ===")
df_raw.show(5, truncate=False)

# =========================================================
# CONVERT DATETIME
# =========================================================

df = df_raw.withColumn(
    "datetime",
    to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
)

print("=== DATETIME CONVERTED ===")

df.select("datetime").show(5, truncate=False)

df.printSchema()

# ==========================================
# NORMALIZACIJA NAZIVA GRADOVA
# ==========================================

df = df.withColumn(
    "city_name",
    trim(col("city_name"))
)

print("=== DISTINCT CITIES ===")
df.select("city_name").distinct().show(20, truncate=False)




# ==========================================
# MONTH-DAY KEY (potrebno je za streamng join, da ne bi morao svaki put komplikovano da izvlacim datum npr. "uporedi trenutno stanje sa istorijskim prosekom za to isto vreme")
# ==========================================

df = df.withColumn(
    "month_day",
    date_format(col("datetime"), "MM-dd")
)

df.select(
    "datetime",
    "month_day"
).show(5, truncate=False)




# ==========================================
# WIND SPEED: km/h -> m/s
# ==========================================

df = (
    df
    .withColumn(
        "wind_speed_10m",
        round(col("wind_speed_10m") / 3.6, 2)
    )
    .withColumn(
        "wind_speed_100m",
        round(col("wind_speed_100m") / 3.6, 2)
    )
    .withColumn(
        "wind_gusts_10m",
        round(col("wind_gusts_10m") / 3.6, 2)
    )
)

print("=== WIND SPEED CONVERTED TO m/s ===")

df.select(
    "wind_speed_10m",
    "wind_speed_100m",
    "wind_gusts_10m"
).show(5)


# ==========================================
# DODAVANJE VREMENSKIH OBELEZJA
# ==========================================

df = (
    df
    .withColumn("year", year(col("datetime")))
    .withColumn("month", month(col("datetime")))
    .withColumn("day", dayofmonth(col("datetime")))
    .withColumn("hour", hour(col("datetime")))
    .withColumn("date", to_date(col("datetime")))
)

print("=== TIME FEATURES ===")

df.select(
    "datetime",
    "year",
    "month",
    "day",
    "hour",
    "date"
).show(5, truncate=False)


# ==========================================
# DAY/NIGHT FLAG
# ==========================================

df = df.withColumn(
    "is_day",
    when(
        (col("hour") >= 6) & (col("hour") < 18),
        True
    ).otherwise(False)
)

df.select(
    "datetime",
    "hour",
    "is_day"
).show(10)



# ==========================================
# SEASON COLUMN
# ==========================================

df = df.withColumn(
    "season",
    when(col("month").isin(12, 1, 2), "winter")
    .when(col("month").isin(3, 4, 5), "spring")
    .when(col("month").isin(6, 7, 8), "summer")
    .otherwise("autumn")
)

df.select(
    "datetime",
    "month",
    "season"
).show(12)



# ==========================================
# WINTER SEASON YEAR
# ==========================================

df = df.withColumn(
    "winter_season_year",
    when(
        col("month").isin(1, 2),
        col("year") - 1
    ).otherwise(col("year"))
)

df.select(
    "datetime",
    "month",
    "year",
    "winter_season_year"
).show(15)



# ==========================================
# WEATHER FLAGS (snow+sunny indikatori)
# ==========================================

df = (
    df
    .withColumn(
        "is_snow_day",
        when(col("snowfall") > 0, True)
        .otherwise(False)
    )
    .withColumn(
        "is_sunny_hour",
        when(
            (col("direct_radiation") > 120) &
            (col("cloud_cover") < 50),
            True
        ).otherwise(False)
    )
)

df.select(
    "datetime",
    "snowfall",
    "cloud_cover",
    "direct_radiation",
    "is_snow_day",
    "is_sunny_hour"
).show(10)




# ==========================================
# DROP UNUSED COLUMNS
# ==========================================

columns_to_drop = [
    "shortwave_radiation_instant",
    "direct_radiation_instant",
    "diffuse_radiation_instant",
    "direct_normal_irradiance_instant",
    "global_tilted_irradiance_instant",
    "terrestrial_radiation_instant"
]

df = df.drop(*columns_to_drop)

print("=== FINAL SCHEMA ===")
df.printSchema()

# ovo se radi da bi spark rasporedio podatke unapred po year/month, posto i onako 
# radimo .partitionBy("year", "month"). Write je efikasniji, manje prtiska na memory, stabilniji parquet write. Optimization step.
# Reparticionisanje po godini i mesecu radi bolje organizacije podataka
# u Parquet formatu i boljih performansi pri upitima kroz partition pruning u HDFS-u.
df = df.repartition("year", "month")


# ==========================================
# WRITE TO TRANSFORMED ZONE (upis u transformed zonu)
# ==========================================

TRANSFORMED_PATH = (
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

(
    df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(TRANSFORMED_PATH)
)

print("=== DATA WRITTEN TO TRANSFORMED ZONE ===")




# ==========================================
# VALIDATION
# ==========================================
# Validacija upisanih Parquet podataka bez punog skeniranja dataseta.
# Provera uspesnog upisa transformisanih podataka.
# Koristimo samo sample i schema prikaz,
# jer count() zahteva potpuno skeniranje svih Parquet fajlova
# i predstavlja skupu operaciju za veliki dataset.

df_check = spark.read.parquet(TRANSFORMED_PATH)

print("=== TRANSFORMED SAMPLE ===")
df_check.show(5)

print("=== TRANSFORMED SCHEMA ===")
df_check.printSchema()













