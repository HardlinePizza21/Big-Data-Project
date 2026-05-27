# Punto 7 — Procesamiento Analítico con PySpark

## Descripción

Se desarrolló un script PySpark (`analisis_pyspark.py`) que responde las 5 preguntas de negocio del proyecto usando procesamiento distribuido sobre los datasets almacenados en S3.

## Datasets utilizados

- `s3://smadrido/meteo-enriched/` — datos meteorológicos horarios de 392 estaciones (2020–2024), formato Parquet particionado por year/month/estacion_id
- `s3://smadrido/store/Calidad_del_Aire_en_Colombia_20260516.csv` — mediciones de calidad del aire del IDEAM (~7GB, +6 millones de registros de PM2.5 y PM10)

## Preguntas respondidas

| Query | Pregunta | Dataset |
|-------|----------|---------|
| Q1 | ¿Qué estación tiene el mayor heat index promedio? | meteo-enriched |
| Q2 | ¿Es más caluroso en la primera o segunda quincena del mes? | meteo-enriched |
| Q3 | ¿Qué estaciones registran mayor precipitación acumulada anual? | meteo-enriched |
| Q4 | ¿Cómo varía la humedad relativa por mes y qué relación tiene con la temperatura aparente? | meteo-enriched |
| Q5 | ¿Qué municipios tienen las mayores concentraciones de PM2.5 y PM10? | Calidad del Aire IDEAM |

## Infraestructura

El script se ejecutó en un cluster **Amazon EMR** (emr-spark-8.0.0) con la siguiente configuración:

- 1 nodo Primary (m5.xlarge)
- 1 nodo Core (r8g.xlarge)
- 1 nodo Task (r8g.xlarge)
- Spark 4.0.2
- Integración con AWS Glue Data Catalog

## Ejecución

```bash
# Subir script a S3
aws s3 cp analisis_pyspark.py s3://smadrido/scripts/analisis_pyspark.py

# Lanzar step en EMR
aws emr add-steps \
  --cluster-id <CLUSTER_ID> \
  --steps Type=Spark,Name="AnalisisQ5",ActionOnFailure=CONTINUE,\
Args=[s3://smadrido/scripts/analisis_pyspark.py]

# Monitorear
aws emr describe-step \
  --cluster-id <CLUSTER_ID> \
  --step-id <STEP_ID> \
  --query 'Step.Status.State'
```

## Resultados

Los resultados de cada query se guardaron como archivos Parquet en S3:

```
s3://smadrido/resultados/
├── q1_heat_index/
├── q2_quincenas_estacion/
├── q2_quincenas_global/
├── q3_precipitacion_anual/
├── q3_precipitacion_consolidado/
├── q4_humedad_temperatura/
└── q5_pm_municipios/
```

El step finalizó con estado **COMPLETED** procesando ~16 millones de filas distribuidas entre los nodos del cluster.
