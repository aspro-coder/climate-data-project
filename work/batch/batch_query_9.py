"""
Upit 9: Oscilacije vlaznosti vazduha izmedju dana i noci
u periodu 1990–1999.

Analiza identifikuje gradove sa najvecim razlikama izmedju
dnevne i nocne vlaznosti, uz rangiranje po godinama i kroz ceo period.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max, min, avg, round,
    when, rank
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("humidity_oscillation_ranking")
    .getOrCreate()
)

# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Izracunavanje dnevnih ekstremnih vrednosti vlaznosti
# (razdvajamo dnevne i nocne sate preko is_day kolone)
daily_humidity = (
    df.groupBy("city_name", "date", "year")
    .agg(
        max(when(col("is_day") == True,
            col("relative_humidity_2m"))).alias("max_day_humidity"),
        min(when(col("is_day") == False,
            col("relative_humidity_2m"))).alias("min_night_humidity")
    )
    .filter(
        col("max_day_humidity").isNotNull() &
        col("min_night_humidity").isNotNull()
    )
)

# Dnevna oscilacija vlaznosti (razlika dan–noc)
daily_humidity = daily_humidity.withColumn(
    "humidity_oscillation",
    round(col("max_day_humidity") - col("min_night_humidity"), 2)
)

# Agregacija po gradu i godini
# (pratimo kako se oscilacije menjaju kroz vreme)
yearly_stats = (
    daily_humidity
    .groupBy("city_name", "year")
    .agg(
        round(avg("humidity_oscillation"), 2)
            .alias("avg_humidity_oscillation"),
        round(max("humidity_oscillation"), 2)
            .alias("max_humidity_oscillation"),
        round(min("humidity_oscillation"), 2)
            .alias("min_humidity_oscillation")
    )
)

# Rangiranje unutar svake godine po prosecnoj oscilaciji
window_by_year = (
    Window
    .partitionBy("year")
    .orderBy(col("avg_humidity_oscillation").desc())
)

yearly_stats = yearly_stats.withColumn(
    "rank_in_year",
    rank().over(window_by_year)
)

# Agregacija kroz ceo period (jedna vrednost po gradu)
overall_stats = (
    yearly_stats
    .groupBy("city_name")
    .agg(
        round(avg("avg_humidity_oscillation"), 2)
            .alias("overall_avg_oscillation")
    )
)

# Globalno rangiranje gradova po ukupnoj oscilaciji
window_overall = Window.orderBy(
    col("overall_avg_oscillation").desc()
)

overall_stats = overall_stats.withColumn(
    "overall_rank",
    rank().over(window_overall)
)

# Spajanje godisnjih i ukupnih rezultata
result = (
    yearly_stats
    .join(overall_stats, on="city_name", how="left")
    .orderBy("year", "rank_in_year")
)

# Prikaz rezultata
result.show(20, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "humidity_oscillation_ranking")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
