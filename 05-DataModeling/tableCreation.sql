CREATE EXTERNAL TABLE storeGov (
    estacion_id INT,
    nombre_fgda STRING,
    nombre_est STRING,
    msfl_code STRING,
    med_concentracion_estandar DOUBLE,

    med_fecha_inicio TIMESTAMP,
    med_fecha_final TIMESTAMP,

    latitud DOUBLE,
    longitud DOUBLE,
    altitud DOUBLE,

    nombre_unidad STRING,
    sigla_unidad STRING,

    duracion INT,
    codigo_departamento INT,

    departamento STRING,

    codigo_municipio STRING,
    municipio STRING,

    tipo_estacion STRING
)
STORED AS PARQUET
LOCATION 's3://smadrido/clean-data/';


-- ─────────────────────────────────────────────────────────────────────────────
-- queries_analisis.sql
-- Queries Hive / Athena sobre s3://smadrido/meteo-enriched/
-- Particionado: year= / month= / estacion_id=
-- ─────────────────────────────────────────────────────────────────────────────

-- Tabla externa en Hive/Athena (ejecutar una vez)
CREATE EXTERNAL TABLE IF NOT EXISTS meteo_enriched (
    estacion_id          INT,
    timestamp            TIMESTAMP,
    day                  INT,
    temperature_2m       FLOAT,
    apparent_temperature FLOAT,
    relative_humidity_2m FLOAT,
    wind_speed_10m       FLOAT,
    precipitation        FLOAT,
    weather_code         SMALLINT,
    pm10                 FLOAT,
    pm2_5                FLOAT,
    carbon_monoxide      FLOAT,
    nitrogen_dioxide     FLOAT,
    sulphur_dioxide      FLOAT,
    ozone                FLOAT,
    us_aqi               SMALLINT,
    lat                  DOUBLE,
    lon                  DOUBLE
)
PARTITIONED BY (year INT, month INT, estacion_id INT)
STORED AS PARQUET
LOCATION 's3://smadrido/meteo-enriched/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Reparar particiones después de cada ingesta
MSCK REPAIR TABLE meteo_enriched;