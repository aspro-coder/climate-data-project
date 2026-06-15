"""
Upit: Kako se prosecni pritisak i padavine menjaju tokom letnjih i zimskih
sezona kroz godine (1990-1999).

Analiza prati medjugodisnje promene unutar istih sezona kako bi se uocili
trendovi i odstupanja u klimatskim indikatorima.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, round, lag, when
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("seasonal_pressure_precipitation")
    .getOrCreate()
)

# Ucitavanje transformisanih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Zadrzavamo samo letnje i zimske mesece
seasonal_df = df.filter(col("season").isin("summer", "winter"))

# Agregacija osnovnih klimatskih indikatora po gradu, godini i sezoni
seasonal_stats = (
    seasonal_df
    .groupBy("city_name", "year", "season")
    .agg(
        round(avg("pressure_msl"), 2).alias("avg_pressure"),
        round(avg("precipitation") * 24, 2).alias("avg_daily_precipitation"),
        round(avg("temperature_2m"), 2).alias("avg_temperature")
    )
)

# Window definicija-poredjenje kroz godine unutar istog grada i sezone
window_city_season = (
    Window
    .partitionBy("city_name", "season")
    .orderBy("year")
)

# Racunanje promena u odnosu na prethodnu godinu (leto sa letom, zima sa zimom)
result = (
    seasonal_stats

    .withColumn("prev_year_pressure",
        lag("avg_pressure", 1).over(window_city_season))
    .withColumn("prev_year_precipitation",
        lag("avg_daily_precipitation", 1).over(window_city_season))

    .withColumn("pressure_change_vs_prev_year",
        round(
            when(col("prev_year_pressure").isNotNull(),
                col("avg_pressure") - col("prev_year_pressure")
            ).otherwise(0.0),
        2))
    .withColumn("precipitation_change_vs_prev_year",
        round(
            when(col("prev_year_precipitation").isNotNull(),
                col("avg_daily_precipitation") - col("prev_year_precipitation")
            ).otherwise(0.0),
        2))

    .drop("prev_year_pressure", "prev_year_precipitation")
    .orderBy("city_name", "season", "year")
)

# Prikaz rezultata
result.show(20, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "seasonal_pressure_precipitation")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
