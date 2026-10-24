from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, desc, sum, count, avg, max

spark = SparkSession.builder \
    .appName("DisneyCleanAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "disney_topic") \
    .option("startingOffsets", "earliest") \
    .load()

df_string = df_raw.selectExpr("CAST(value AS STRING) as raw_data")

# Parseamos de forma segura las 3 columnas exactas
df_parsed = df_string.select(
    split(col("raw_data"), ",")[0].alias("movie_title"),
    split(col("raw_data"), ",")[1].alias("genre"),
    split(col("raw_data"), ",")[2].cast("long").alias("inflation_adjusted_gross")
).filter(~col("genre").contains("-")) # Filtra fechas por si acaso

# Agrupamos por género con métricas financieras claras
df_agg = df_parsed.groupBy("genre") \
    .agg(
        count("movie_title").alias("total_peliculas"),
        sum("inflation_adjusted_gross").alias("recaudacion_total"),
        avg("inflation_adjusted_gross").alias("recaudacion_promedio"),
        max("inflation_adjusted_gross").alias("pelicula_mas_taquillera")
    ) \
    .orderBy(desc("recaudacion_total"))

query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
