"""
Upit 10: Odnos izmedju temperature i kolicine padavina u gradovima
u periodu 1990-1999.

Analiza ispituje da li gradovi sa vecim padavinama imaju nize
ili vise prosecne temperature, koristeci korelaciju i kvartilnu
podelu podataka.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, round, ntile, corr
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("temp_precipitation_correlation")
    .getOrCreate()
)


# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Agregacija klimatskih pokazatelja po gradu kroz ceo period
city_stats = (
    df.groupBy("city_name")
    .agg(
        round(avg("temperature_2m"), 2).alias("avg_temperature"),
        round(avg("precipitation") * 24, 2).alias("avg_daily_precipitation"),
        round(avg("relative_humidity_2m"), 2).alias("avg_humidity")
    )
)

# Podela gradova u kvartile prema kolicini padavina
# (Q1 = najsusniji gradovi, Q4 = najkisovitiji)
window_for_ntile = Window.orderBy("avg_daily_precipitation")

city_stats = city_stats.withColumn(
    "precipitation_quartile",
    ntile(4).over(window_for_ntile)
)

# Labele za laksu interpretaciju kvartila
from pyspark.sql.functions import when

city_stats = city_stats.withColumn(
    "quartile_label",
    when(col("precipitation_quartile") == 1, "Q1 - Najsuvlji")
    .when(col("precipitation_quartile") == 2, "Q2 - Suvlji")
    .when(col("precipitation_quartile") == 3, "Q3 - Kišovitiji")
    .when(col("precipitation_quartile") == 4, "Q4 - Najkišovitiji")
)

# Globalna Pearson korelacija izmedju temperature i padavina
correlation = df.select(
    corr("temperature_2m", "precipitation").alias("pearson_correlation")
).collect()[0]["pearson_correlation"]

print(f"\nPearsonov koeficijent korelacije: {correlation: .4f}")

# Analiza prosecnih vrednosti po kvartilima padavina
quartile_summary = (
    city_stats
    .groupBy("precipitation_quartile", "quartile_label")
    .agg(
        round(avg("avg_temperature"), 2).alias("avg_temp_in_quartile"),
        round(avg("avg_daily_precipitation"), 2).alias("avg_precip_in_quartile"),
        round(avg("avg_humidity"), 2).alias("avg_humidity_in_quartile")
    )
    .orderBy("precipitation_quartile")
)

quartile_summary.show(truncate=False)

# Upisujemo dva rezultata:
# 1. city_stats — detalji po gradu sa kvartilarnom oznakom
# 2. quartile_summary — sažetak po kvartilu
(
    city_stats
    .orderBy("precipitation_quartile", "avg_daily_precipitation")
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "temp_precipitation_correlation")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

(
    quartile_summary.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "temp_precipitation_quartile_summary")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
