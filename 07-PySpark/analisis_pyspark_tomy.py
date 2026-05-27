"""
analisis_pyspark.py
Procesamiento analítico descriptivo con PySpark
Dataset: s3://smadrido/meteo-enriched/
Preguntas de negocio Q1-Q4
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# Inicializar SparkSession con soporte a S3
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MeteoEnriched-Analisis") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

S3_PATH = "s3://smadrido/meteo-enriched/"

# ─────────────────────────────────────────────
# Leer el dataset particionado desde S3
# ─────────────────────────────────────────────
print(">>> Leyendo dataset desde S3...")
df = spark.read.parquet(S3_PATH)

# Filtrar solo años con datos completos
df = df.filter(F.col("year").between(2020, 2024))

df.cache()
print(f">>> Total filas cargadas: {df.count():,}")
df.printSchema()


# ─────────────────────────────────────────────────────────────────────────────
# Q1: ¿Qué estación tiene el mayor heat index (apparent_temperature) promedio?
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
q1.write.mode("overwrite").parquet("s3://smadrido/resultados/q1_heat_index/")
print(">>> Q1 guardado en s3://smadrido/resultados/q1_heat_index/")


# ─────────────────────────────────────────────────────────────────────────────
# Q2: ¿Es más caluroso en la primera o segunda quincena del mes?
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("Q2: Comparación de temperatura por quincena")
print("="*70)

df_q2 = df.filter(F.col("apparent_temperature").isNotNull()) \
    .withColumn(
        "quincena",
        F.when(F.col("day") <= 15, "1ra_quincena").otherwise("2da_quincena")
    )

# Por estación
q2_estacion = df_q2.groupBy("estacion_id", "quincena") \
    .agg(
        F.avg("apparent_temperature").alias("avg_apparent_temp"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.stddev_pop("apparent_temperature").alias("std_apparent_temp"),
        F.count("*").alias("n_horas")
    ) \
    .orderBy("estacion_id", "quincena")

q2_estacion.show(40, truncate=False)

# Resumen global
q2_global = df_q2.groupBy("quincena") \
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.count("*").alias("n_horas")
    ) \
    .orderBy("quincena")

print("--- Resumen global ---")
q2_global.show(truncate=False)

q2_estacion.write.mode("overwrite").parquet("s3://smadrido/resultados/q2_quincenas_estacion/")
q2_global.write.mode("overwrite").parquet("s3://smadrido/resultados/q2_quincenas_global/")
print(">>> Q2 guardado en s3://smadrido/resultados/q2_quincenas_*/")


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
    ) \
    .orderBy(F.col("precip_total_mm").desc()) \
    .limit(20)

q3.show(truncate=False)

# Ranking consolidado por estación (promedio entre años)
q3_consolidado = df.filter(F.col("precipitation").isNotNull()) \
    .groupBy("estacion_id") \
    .agg(
        F.sum("precipitation").alias("precip_total_5anos"),
        F.avg("precipitation").alias("precip_avg_hora_global"),
        F.count("*").alias("n_horas_total")
    ) \
    .orderBy(F.col("precip_total_5anos").desc()) \
    .limit(20)

print("--- Top 20 estaciones más lluviosas (5 años acumulado) ---")
q3_consolidado.show(truncate=False)

q3.write.mode("overwrite").parquet("s3://smadrido/resultados/q3_precipitacion_anual/")
q3_consolidado.write.mode("overwrite").parquet("s3://smadrido/resultados/q3_precipitacion_consolidado/")
print(">>> Q3 guardado en s3://smadrido/resultados/q3_precipitacion_*/")


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
    ) \
    .groupBy("year", "month") \
    .agg(
        F.avg("apparent_temperature").alias("avg_heat_index"),
        F.avg("relative_humidity_2m").alias("avg_humidity"),
        F.avg("temperature_2m").alias("avg_temp_real"),
        F.count("*").alias("n_horas")
    ) \
    .orderBy("year", "month")

q4.show(60, truncate=False)

# Correlación de Pearson entre humedad y temperatura aparente
corr_value = df.filter(
    F.col("apparent_temperature").isNotNull() &
    F.col("relative_humidity_2m").isNotNull()
).stat.corr("relative_humidity_2m", "apparent_temperature")

print(f"\n>>> Correlación de Pearson (humedad vs heat index): {corr_value:.4f}")

q4.write.mode("overwrite").parquet("s3://smadrido/resultados/q4_humedad_temperatura/")
print(">>> Q4 guardado en s3://smadrido/resultados/q4_humedad_temperatura/")


print("\n>>> Análisis completado.")
spark.stop()
