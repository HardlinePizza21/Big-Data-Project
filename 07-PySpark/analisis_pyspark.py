"""
analisis_pyspark.py
────────────────────────────────────────────────────────────────────────────────
Análisis con PySpark sobre los parquets meteo-enriched en S3.
Equivalente al queries_analisis.sql pero en Python, con join opcional
contra los datos originales del MADS (clean-data/).
────────────────────────────────────────────────────────────────────────────────
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("CalidadAire_Analisis")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

# ──────────────────────────────────────────────────────────────────────────────
# Cargar datos enriquecidos (Open-Meteo)
# ──────────────────────────────────────────────────────────────────────────────
meteo = (
    spark.read
    .option("mergeSchema", "true")
    .parquet("s3://smadrido/meteo-enriched/")
)

# Cargar datos del gobierno (MADS)
mads = (
    spark.read
    .option("mergeSchema", "true")
    .parquet("s3://smadrido/clean-data/")
)

# Añadir columna quincena
meteo = meteo.withColumn(
    "quincena",
    F.when(F.col("day") <= 15, "1ra_quincena").otherwise("2da_quincena"),
)

# ──────────────────────────────────────────────────────────────────────────────
# Q1: Estación con mayor heat index promedio
# ──────────────────────────────────────────────────────────────────────────────
heat_index_ranking = (
    meteo
    .filter(F.col("apparent_temperature").isNotNull())
    .filter(F.col("year").between(2022, 2024))
    .groupBy("estacion_id")
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.max("apparent_temperature").alias("max_heat_index"),
        F.expr("percentile_approx(apparent_temperature, 0.9)").alias("p90_heat_index"),
        F.count("*").alias("n_horas"),
    )
    .orderBy(F.desc("avg_heat_index"))
)

print("=== Q1: Ranking por heat index promedio ===")
heat_index_ranking.show(20, truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Q2: Primera vs segunda quincena del mes
# ──────────────────────────────────────────────────────────────────────────────
quincena_global = (
    meteo
    .filter(F.col("apparent_temperature").isNotNull())
    .filter(F.col("year").between(2022, 2024))
    .groupBy("quincena")
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.stddev_pop("apparent_temperature").alias("std_heat_index"),
        F.count("*").alias("n_horas"),
    )
    .orderBy("quincena")
)

quincena_por_estacion = (
    meteo
    .filter(F.col("apparent_temperature").isNotNull())
    .filter(F.col("year").between(2022, 2024))
    .groupBy("estacion_id", "quincena")
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.count("*").alias("n_horas"),
    )
    .orderBy("estacion_id", "quincena")
)

print("=== Q2a: Quincena global ===")
quincena_global.show(truncate=False)

print("=== Q2b: Quincena por estación ===")
quincena_por_estacion.show(50, truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Q3: Join con datos MADS para enriquecer con concentración real de contaminantes
# ──────────────────────────────────────────────────────────────────────────────
mads_hourly = (
    mads
    .withColumn("hour_ts",
        F.date_trunc("hour", F.col("med_fecha_inicio")))
    .groupBy("estacion_id", "msfl_code", "hour_ts")
    .agg(F.avg("med_concentracion_estandar").alias("concentracion"))
)

meteo_hourly = (
    meteo
    .withColumn("hour_ts",
        F.date_trunc("hour", F.col("timestamp")))
)

joined = (
    meteo_hourly.alias("m")
    .join(
        mads_hourly.alias("g"),
        on=["estacion_id", "hour_ts"],
        how="left",
    )
    .select(
        "m.estacion_id",
        "m.hour_ts",
        "m.apparent_temperature",
        "m.temperature_2m",
        "m.relative_humidity_2m",
        "m.us_aqi",
        "m.pm2_5",
        "m.pm10",
        "g.msfl_code",
        "g.concentracion",
        "m.year",
        "m.month",
        "m.quincena",
    )
)

print("=== Q3: Dataset enriquecido (MADS + Open-Meteo) ===")
joined.printSchema()
joined.show(10, truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Q4: Tendencia mensual — heat index vs AQI
# ──────────────────────────────────────────────────────────────────────────────
tendencia_mensual = (
    meteo
    .filter(F.col("apparent_temperature").isNotNull())
    .filter(F.col("us_aqi").isNotNull())
    .groupBy("year", "month")
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("us_aqi").alias("avg_aqi"),
        F.avg("pm2_5").alias("avg_pm25"),
    )
    .orderBy("year", "month")
)

print("=== Q4: Tendencia mensual heat index vs AQI ===")
tendencia_mensual.show(50, truncate=False)

# Guardar resultados como parquet para visualización posterior
joined.write.mode("overwrite").parquet(
    "s3://smadrido/analisis-output/joined_mads_meteo/"
)
tendencia_mensual.write.mode("overwrite").parquet(
    "s3://smadrido/analisis-output/tendencia_mensual/"
)
heat_index_ranking.write.mode("overwrite").parquet(
    "s3://smadrido/analisis-output/heat_index_ranking/"
)

spark.stop()
