CREATE EXTERNAL TABLE storeGov (
    estacion_id INT,
    nombre_fgda STRING,
    nombre_est STRING,
    msfl_code STRING,
    med_concentracion_estandar INT,

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