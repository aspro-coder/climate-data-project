"""
Upit 8: Analiza padavina tokom dana sa visokim temperaturama (>30°C)
u periodu 1990-1999.

Cilj je da se ispita koliko cesto se padavine javljaju tokom
izuzetno toplih dana, po godinama i ukupno po gradovima.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max, sum as spark_sum, count,
    round, when, lit
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("hot_day_precipitation")
    .getOrCreate()
)

# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Agregacija dnevnih vrednosti (podaci su originalno satni)
daily_stats = (
    df.groupBy("city_name", "date", "year")
    .agg(
        max("temperature_2m").alias("max_temp"),
        round(spark_sum("precipitation"), 2).alias("total_precip")
    )
)

# Zadrzavamo samo dane sa ekstremno visokim temperaturama (>30°C)
hot_days = daily_stats.filter(col("max_temp") > 30)

# Obelezavanje dana sa merljivim padavinama
hot_days = hot_days.withColumn(
    "has_precipitation",
    when(col("total_precip") > 0.1, 1).otherwise(0)
)

# Godisnja agregacija po gradu
yearly_stats = (
    hot_days
    .groupBy("city_name", "year")
    .agg(
        count(lit(1)).alias("total_hot_days"),
        spark_sum("has_precipitation").alias("hot_days_with_rain"),
        round(spark_sum("total_precip"), 2).alias("total_precip_on_hot_days")
    )
    .withColumn("rain_pct_yearly",
        round(
            col("hot_days_with_rain") / col("total_hot_days") * 100,
        2))
)


# Ukupna statistika kroz ceo period (po gradu)
window_by_city = Window.partitionBy("city_name")

result = (
    yearly_stats
    # Ukupan broj vrućih dana kroz ceo period za taj grad
    .withColumn("total_hot_days_all_years",
        spark_sum("total_hot_days").over(window_by_city))
    # Ukupan broj vrućih dana sa kišom kroz ceo period
    .withColumn("total_hot_rainy_days_all_years",
        spark_sum("hot_days_with_rain").over(window_by_city))
    # Ukupne padavine na vrućim danima kroz ceo period
    .withColumn("total_precip_all_years",
        round(spark_sum("total_precip_on_hot_days").over(window_by_city), 2))
    # Procenat vrućih dana sa kišom kroz ceo period
    .withColumn("rain_pct_all_years",
        round(
            col("total_hot_rainy_days_all_years") /
            col("total_hot_days_all_years") * 100,
        2))
    .orderBy("city_name", "year")
)

# Prikaz rezultata
result.show(20, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "hot_day_precipitation")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
