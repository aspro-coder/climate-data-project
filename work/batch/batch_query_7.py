"""
Upit: Koji gradovi imaju najmanje suncanih sati tokom zimskih sezona
(1990-1999), i kako se rangiraju po svakoj zimi.

Analiza identifikuje gradove sa najmanjim brojem suncanih sati
u zimskim mesecima i rangira ih unutar svake zimske sezone.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, round, dense_rank, when, lit
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("least_sunny_winters")
    .getOrCreate()
)


# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Filtriranje zimskih sezona (decembar–februar)
winter_df = df.filter(col("season") == "winter")

# Agregacija suncanih sati po gradu i zimskoj sezoni
sunny_stats = (
    winter_df
    .groupBy("city_name", "winter_season_year")
    .agg(
        count(when(col("is_sunny_hour") == True, 1))
            .alias("sunny_hours_count"),
        count(lit(1))
            .alias("total_hours")
    )
    .withColumn("sunny_hours_pct",
        round(
            col("sunny_hours_count") / col("total_hours") * 100,
        2))
)

# Rangiranje gradova unutar svake zimske sezone po broju suncanih sati
window_by_winter = (
    Window
    .partitionBy("winter_season_year")
    .orderBy("sunny_hours_count")
)

result = (
    sunny_stats
    .withColumn("rank_in_winter",
        dense_rank().over(window_by_winter))
    .orderBy("winter_season_year", "rank_in_winter")
)

# Prikaz rezultata
result.show(20, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "least_sunny_winters")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
