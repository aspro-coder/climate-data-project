"""
Upit: Koji gradovi imaju najvise sneznih dana tokom zimskih sezona
u periodu 1990-1999, i kako se rangiraju po svakoj zimi.

Analiza identifikuje gradove sa najvecom ucestaloscu sneznih dana
i rangira ih unutar svake zimske sezone.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, dense_rank, desc
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("snowy_winters_ranking")
    .getOrCreate()
)

# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Filtriramo samo zimske mesece, jer podaci vec imaju definisanu sezonu
winter_df = df.filter(col("season") == "winter")

# Racunamo broj dana sa snegom po gradu i zimskoj sezoni
snow_days = (
    winter_df
    .filter(col("is_snow_day") == True)
    .groupBy("city_name", "winter_season_year")
    .agg(
        countDistinct("date").alias("snow_days_count")
    )
)

# Rangiranje gradova unutar svake zimske sezone po broju sneznih dana
window_by_winter = (
    Window
    .partitionBy("winter_season_year")
    .orderBy(desc("snow_days_count"))
)

# Prikaz rezultata
result = (
    snow_days
    .withColumn("rank_in_winter", dense_rank().over(window_by_winter))
    .orderBy("winter_season_year", "rank_in_winter")
)

result.show(30, truncate=False)


# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "snowy_winters_ranking")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
