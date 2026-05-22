SELECT
    CAST(estacion_id AS INT) AS estacion_id,
    nombre_fgda,
    nombre_est,
    msfl_code,
    CAST(med_concentracion_estandar AS INT) AS med_concentracion_estandar,

    to_timestamp(
        med_fecha_inicio,
        'yyyy MMM dd hh:mm:ss a'
    ) AS med_fecha_inicio,

    to_timestamp(
        med_fecha_final,
        'yyyy MMM dd hh:mm:ss a'
    ) AS med_fecha_final,

    CAST(latitud AS DOUBLE) AS latitud,
    CAST(longitud AS DOUBLE) AS longitud,
    CAST(altitud AS DOUBLE) AS altitud,

    nombre_unidad,
    sigla_unidad,
    CAST(`duración` AS INT) AS duracion,
    CAST(codigo_departamento AS INT) AS codigo_departamento,

    departamento,

    codigo_municipio,
    municipio,
    tipo_estacion

FROM myDataSource