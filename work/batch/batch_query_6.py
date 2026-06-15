"""
Upit: Kako se prosecna brzina vetra menja izmedju sezona
u gradovima tokom perioda 1990-1999.

Analiza prati sezonske promene vetra unutar svake godine,
kako bi se identifikovale razlike izmedju uzastopnih sezona.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, round, lag, when, lit
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("wind_speed_seasonal_changes")
    .getOrCreate()
)

# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Agregacija prosecne brzine vetra po gradu, godini i sezoni
seasonal_wind = (
    df.groupBy("city_name", "year", "season")
    .agg(
        round(avg("wind_speed_10m"), 2).alias("avg_wind_speed_10m"),
        round(avg("wind_speed_100m"), 2).alias("avg_wind_speed_100m"),
        round(avg("wind_gusts_10m"), 2).alias("avg_wind_gusts")
    )
)

# Mapiranje sezona u numericki redosled radi hronoloskog poredjenja
seasonal_wind = seasonal_wind.withColumn(
    "season_order",
    when(col("season") == "spring", 1)
    .when(col("season") == "summer", 2)
    .when(col("season") == "autumn", 3)
    .when(col("season") == "winter", 4)
    .otherwise(0)
)

# Window definicija: poredjenje sezona unutar istog grada i godine
window_city_year = (
    Window
    .partitionBy("city_name", "year")
    .orderBy("season_order")
)

# Racunanje promene brzine vetra u odnosu na prethodnu sezonu
result = (
    seasonal_wind

    .withColumn("prev_season_wind",
        lag("avg_wind_speed_10m", 1).over(window_city_year))

    .withColumn("wind_change_vs_prev_season",
        round(
            when(col("prev_season_wind").isNotNull(),
                col("avg_wind_speed_10m") - col("prev_season_wind")
            ).otherwise(0.0),
        2))

    .drop("prev_season_wind")

    .orderBy("city_name", "year", "season_order")
)

# Prikaz rezultata
result.show(20, truncate=False)


# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "wind_speed_seasonal_changes")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
