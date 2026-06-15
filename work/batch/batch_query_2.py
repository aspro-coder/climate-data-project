"""
Upit: Kako se klimatski uslovi (temperatura, vlaznost, padavine,
vetar i oblacnost) menjaju po gradovima na mesecnom nivou
u periodu 1990-1999.

Analiza daje pregled osnovnih klimatskih obrazaca kroz mesecne
proseke za svaki grad.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Kreiranje Spark sesije za analizu klimatskih trendova po mesecima
spark = (
    SparkSession.builder
    .appName("monthly_climate_trends")
    .getOrCreate()
)

# Ucitavanje transformisanih podataka iz HDFS (1990–1999)
df = spark.read.parquet(
    "hdfs://namenode:9000/data/transformed_zone/transformed_weather_data"
)

# Agregacija podataka po gradu i mesecu
result = (
    df.groupBy("city_name", "month")
    .agg(
        round(avg("temperature_2m"), 2).alias("avg_temperature"),
        round(avg("relative_humidity_2m"), 2).alias("avg_humidity"),
        round(avg("precipitation") * 24, 2).alias("avg_daily_precipitation"),
        round(avg("wind_speed_10m"), 2).alias("avg_wind_speed"),
        round(avg("cloud_cover"), 2).alias("avg_cloud_cover")
    )
    .orderBy("city_name", "month")
)

result.show(24, truncate=False)

# Upis agregiranih rezultata u PostgreSQL bazu
(
    result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgresql:5432/postgres")
    .option("dbtable", "monthly_climate_trends")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
