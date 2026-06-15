"""
Upit: Identifikacija gradova sa najvecim brojem ekstremnih
temperaturnih dana u periodu 1990-1999.

Ekstremni dani su definisani kao dani sa minimalnom temperaturom
ispod -10°C ili maksimalnom iznad 40°C, uz rangiranje gradova
po godinama.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, dense_rank, desc,
    min, max, when, lit, sum as spark_sum
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("extreme_temp_days")
    .getOrCreate()
)


# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Racunanje dnevnih minimuma i maksimuma temperature (podaci su satni, pa agregiramo na nivo dana)
daily_temps = (
    df.groupBy("city_name", "date", "year")
    .agg(
        min("temperature_2m").alias("daily_min_temp"),
        max("temperature_2m").alias("daily_max_temp")
    )
)


# Oznacavanje ekstremnih dana na osnovu temperaturnih pragova
daily_temps = (
    daily_temps
    .withColumn("is_extreme_cold",
        when(col("daily_min_temp") < -10, True).otherwise(False))
    .withColumn("is_extreme_heat",
        when(col("daily_max_temp") > 40, True).otherwise(False))
    .withColumn("is_extreme",
        col("is_extreme_cold") | col("is_extreme_heat"))
)


# Agregacija broja ekstremnih dana po gradu i godini
extreme_counts = (
    daily_temps
    .filter(col("is_extreme") == True)
    .groupBy("city_name", "year")
    .agg(
        spark_sum(when(col("is_extreme"), 1).otherwise(0))
            .alias("total_extreme_days"),
        spark_sum(when(col("is_extreme_cold"), 1).otherwise(0))
            .alias("extreme_cold_days"),
        spark_sum(when(col("is_extreme_heat"), 1).otherwise(0))
            .alias("extreme_heat_days")
    )
)

# Rangiranje gradova unutar svake godine po broju ekstremnih dana
window_by_year = (
    Window
    .partitionBy("year")
    .orderBy(desc("total_extreme_days"))
)

# Prikaz rezultata
result = (
    extreme_counts
    .withColumn("rank_in_year", dense_rank().over(window_by_year))
    .orderBy("year", "rank_in_year")
)

result.show(30, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "extreme_temp_days")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
