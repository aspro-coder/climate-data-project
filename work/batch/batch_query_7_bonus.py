"""
Upit 7 dodatni: Odstupanje zimskih sunčanih sati u odnosu na dugoročni prosek
         za svaki grad u periodu 1990-1999.


Analiza poredi svaku zimsku sezonu sa tipicnim (prosecnim) uslovima
za isti grad kako bi se identifikovala odstupanja od uobicajenog
obrasca suncanih sati.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, round, avg, when, lit
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("sunny_hours_vs_city_avg")
    .getOrCreate()
)

# Ucitavanje transformisanih meteoroloskih podataka
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Filtriranje zimskih sezona (decembar–februar)
winter_df = df.filter(col("season") == "winter")

# Agregacija suncanih sati po gradu i zimskoj sezoni
# Izracunavamo broj suncanih sati i ukupne sate u sezoni
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

# Racunanje dugorocnog proseka suncanih sati po gradu
# (prosek kroz sve zimske sezone za isti grad)
window_by_city = Window.partitionBy("city_name")

result = (
    sunny_stats
     # Prosecan broj suncanih sati po gradu kroz sve zime
    .withColumn("city_avg_sunny_hours",
        round(avg("sunny_hours_count").over(window_by_city), 2))
    # Odstupanje konkretne zime u odnosu na gradski prosek
    .withColumn("diff_from_city_avg",
        round(
            col("sunny_hours_count") - col("city_avg_sunny_hours"),
        2))
    # Kategorizacija odstupanja u odnosu na prosek
    .withColumn("vs_city_avg",
        when(col("diff_from_city_avg") < -10, "znacajno ispod proseka")
        .when(col("diff_from_city_avg") < 0, "ispod proseka")
        .when(col("diff_from_city_avg") > 10, "znacajno iznad proseka")
        .when(col("diff_from_city_avg") > 0, "iznad proseka")
        .otherwise("prosek"))
    .orderBy("city_name", "winter_season_year")
)

# Prikaz rezultata
result.show(20, truncate=False)

# Upis rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "sunny_hours_vs_city_avg")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
