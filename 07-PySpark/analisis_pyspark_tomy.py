"""
analisis_pyspark.py
Procesamiento analítico descriptivo con PySpark
Datasets:
  - s3://smadrido/meteo-enriched/  (datos meteorológicos Open-Meteo)
  - s3://smadrido/store/Calidad_del_Aire_en_Colombia_20260516.csv (IDEAM)
Preguntas Q1-Q5
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# Inicializar SparkSession
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MeteoCalidadAire-Analisis") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

S3_METEO  = "s3://smadrido/meteo-enriched/"
S3_CSV    = "s3://smadrido/store/Calidad_del_Aire_en_Colombia_20260516.csv"
S3_OUT    = "s3://smadrido/resultados/"

# ─────────────────────────────────────────────
# Leer dataset meteorológico
# ─────────────────────────────────────────────
print(">>> Leyendo dataset meteorológico desde S3...")
df = spark.read.parquet(S3_METEO).filter(F.col("year").between(2020, 2024))
df.cache()
print(f">>> Total filas meteo: {df.count():,}")


# ─────────────────────────────────────────────────────────────────────────────
# Q1: ¿Qué estación tiene el mayor heat index promedio?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q1: Ranking de estaciones por heat index promedio")
print("="*70)

q1 = df.filter(F.col("apparent_temperature").isNotNull()) \
    .groupBy("estacion_id") \
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.max("apparent_temperature").alias("max_heat_index"),
        F.percentile_approx("apparent_temperature", 0.9).alias("p90_heat_index"),
        F.count("*").alias("n_horas")
    ) \
    .orderBy(F.col("avg_heat_index").desc()) \
    .limit(20)

q1.show(truncate=False)
q1.write.mode("overwrite").parquet(S3_OUT + "q1_heat_index/")
print(">>> Q1 guardado")


# ─────────────────────────────────────────────────────────────────────────────
# Q2: ¿Es más caluroso en la primera o segunda quincena del mes?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q2: Comparación de temperatura por quincena")
print("="*70)

df_q2 = df.filter(F.col("apparent_temperature").isNotNull()) \
    .withColumn("quincena",
        F.when(F.col("day") <= 15, "1ra_quincena").otherwise("2da_quincena"))

q2_estacion = df_q2.groupBy("estacion_id", "quincena") \
    .agg(
        F.avg("apparent_temperature").alias("avg_apparent_temp"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.stddev_pop("apparent_temperature").alias("std_apparent_temp"),
        F.count("*").alias("n_horas")
    ).orderBy("estacion_id", "quincena")

q2_global = df_q2.groupBy("quincena") \
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.count("*").alias("n_horas")
    ).orderBy("quincena")

print("--- Resumen global ---")
q2_global.show(truncate=False)

q2_estacion.write.mode("overwrite").parquet(S3_OUT + "q2_quincenas_estacion/")
q2_global.write.mode("overwrite").parquet(S3_OUT + "q2_quincenas_global/")
print(">>> Q2 guardado")


# ─────────────────────────────────────────────────────────────────────────────
# Q3: ¿Qué estaciones registran mayor precipitación acumulada anual?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q3: Ranking de estaciones por precipitación acumulada anual")
print("="*70)

q3 = df.filter(F.col("precipitation").isNotNull()) \
    .groupBy("estacion_id", "year") \
    .agg(
        F.sum("precipitation").alias("precip_total_mm"),
        F.avg("precipitation").alias("precip_avg_hora"),
        F.count("*").alias("n_horas")
    ).orderBy(F.col("precip_total_mm").desc()).limit(20)

q3.show(truncate=False)
q3.write.mode("overwrite").parquet(S3_OUT + "q3_precipitacion_anual/")
print(">>> Q3 guardado")


# ─────────────────────────────────────────────────────────────────────────────
# Q4: ¿Cómo varía la humedad relativa por mes y qué relación tiene
#     con la temperatura aparente?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q4: Humedad relativa vs temperatura aparente por mes")
print("="*70)

q4 = df.filter(
        F.col("apparent_temperature").isNotNull() &
        F.col("relative_humidity_2m").isNotNull()
    ).groupBy("year", "month") \
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("relative_humidity_2m").alias("avg_humidity"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.count("*").alias("n_horas")
    ).orderBy("year", "month")

q4.show(60, truncate=False)

corr = df.filter(
    F.col("apparent_temperature").isNotNull() &
    F.col("relative_humidity_2m").isNotNull()
).stat.corr("relative_humidity_2m", "apparent_temperature")
print(f"\n>>> Correlación Pearson (humedad vs heat index): {corr:.4f}")

q4.write.mode("overwrite").parquet(S3_OUT + "q4_humedad_temperatura/")
print(">>> Q4 guardado")


# ─────────────────────────────────────────────────────────────────────────────
# Q5: ¿Qué municipios tienen las mayores concentraciones promedio de PM2.5
#     y PM10, los contaminantes más peligrosos para la salud respiratoria?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q5: Municipios con mayor concentración de PM2.5 y PM10")
print("="*70)

print(">>> Leyendo dataset calidad del aire IDEAM...")
df_aire = spark.read \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "DROPMALFORMED") \
    .csv(S3_CSV) \
    .withColumn("concentracion", F.regexp_replace(F.col("MED_CONCENTRACION_ESTANDAR"), ",", "").cast("double"))

df_aire.cache()
print(f">>> Total filas calidad aire: {df_aire.count():,}")

df_pm = df_aire.filter(F.col("MSFL_CODE").isin("PM2.5", "PM10"))

q5 = df_pm.groupBy("MUNICIPIO", "DEPARTAMENTO") \
    .agg(
        F.avg(F.when(F.col("MSFL_CODE") == "PM2.5", F.col("concentracion"))).alias("avg_pm25"),
        F.avg(F.when(F.col("MSFL_CODE") == "PM10",  F.col("concentracion"))).alias("avg_pm10"),
        F.count(F.when(F.col("MSFL_CODE") == "PM2.5", 1)).alias("n_pm25"),
        F.count(F.when(F.col("MSFL_CODE") == "PM10",  1)).alias("n_pm10")
    ) \
    .filter(F.col("n_pm25") > 100) \
    .orderBy(F.col("avg_pm25").desc()) \
    .limit(20)

q5.show(truncate=False)
q5.write.mode("overwrite").parquet(S3_OUT + "q5_pm_municipios/")
print(">>> Q5 guardado")


print("\n>>> Análisis completado.")
spark.stop()
