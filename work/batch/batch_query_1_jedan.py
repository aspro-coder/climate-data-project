"""
Upit: Analiza razlike izmedju dnevnih i nocnih temperatura po gradovima
u periodu 1990-1999, sa ciljem identifikacije gradova sa najvecim
prosecnim dnevnim temperaturnim oscilacijama.

Prvo se racunaju dnevne ekstremne vrednosti, zatim se definise
temperaturna oscilacija i na kraju rangiraju gradovi po intenzitetu
oscilacija.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max, min, avg, round, desc, when
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank as spark_rank

# Spark sesija za analizu dnevnih temperaturnih oscilacija
spark = (
    SparkSession.builder
    .appName("diurnal_temp_variation")
    .getOrCreate()
)

# Ucitavanje transformisanih vremenskih podataka iz HDFS
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Racunanje dnevnih ekstremnih vrednosti:
# maksimalna temperatura tokom dana i minimalna tokom noci po gradu i datumu
daily_variation = (
    df.groupBy("city_name", "date")
    .agg(
        max(when(col("is_day") == True,  col("temperature_2m"))).alias("max_day_temp"),
        min(when(col("is_day") == False, col("temperature_2m"))).alias("min_night_temp")
    )
    .filter(
        col("max_day_temp").isNotNull() &
        col("min_night_temp").isNotNull()
    )
)

# Izracunavanje razlike izmedju dnevne i nocne temperature
daily_variation = daily_variation.withColumn(
    "temp_variation",
    round(col("max_day_temp") - col("min_night_temp"), 2)
)

# Agregacija po gradu da bi dobili prosecne i ekstremne oscilacije
city_avg_variation = (
    daily_variation
    .groupBy("city_name")
    .agg(
        round(avg("temp_variation"), 2).alias("avg_temp_variation"),
        round(max("temp_variation"), 2).alias("max_single_day_variation"),
        round(min("temp_variation"), 2).alias("min_single_day_variation")
    )
)

# Rangiranje gradova po prosecnoj temperaturnoj oscilaciji
window_spec = Window.orderBy(desc("avg_temp_variation"))

result = (
    city_avg_variation
    .withColumn("rank", spark_rank().over(window_spec))
    .orderBy("rank")
)

# Prikaz top 20 gradova sa najvecim oscilacijama
result.show(20, truncate=False)

# Cuvanje rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "diurnal_temp_variation")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
