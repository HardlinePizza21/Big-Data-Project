-- 1. Which station has the highest average heat index (apparent temperature)?

SELECT
    estacion_id,
    AVG(apparent_temperature)                        AS avg_heat_index,
    MAX(apparent_temperature)                        AS max_heat_index,
    APPROX_PERCENTILE(apparent_temperature, 0.9)     AS p90_heat_index,
    COUNT(*)                                         AS n_horas
FROM meteo_enriched
WHERE year BETWEEN 2020 AND 2024
  AND apparent_temperature IS NOT NULL
GROUP BY estacion_id
ORDER BY avg_heat_index DESC
LIMIT 20;


-- 2. Is it hotter during the first or second half of the month?

SELECT
    estacion_id,
    CASE WHEN day <= 15 THEN 'first_half' ELSE 'second_half' END AS half_month,
    AVG(apparent_temperature)        AS avg_apparent_temp,
    AVG(temperature_2m)              AS avg_temp_real,
    STDDEV_POP(apparent_temperature) AS std_apparent_temp,
    COUNT(*)                         AS n_horas
FROM meteo_enriched
WHERE year BETWEEN 2020 AND 2024
  AND apparent_temperature IS NOT NULL
GROUP BY
    estacion_id,
    CASE WHEN day <= 15 THEN 'first_half' ELSE 'second_half' END
ORDER BY estacion_id, half_month;


-- 3. Which stations record the highest annual accumulated precipitation and how does it vary across years?

SELECT
    estacion_id,
    year,
    SUM(precipitation)              AS precip_total_mm,
    AVG(precipitation)              AS precip_avg_hora,
    COUNT(*)                        AS n_horas
FROM meteo_enriched
WHERE precipitation IS NOT NULL
GROUP BY estacion_id, year
ORDER BY precip_total_mm DESC
LIMIT 20;


-- 4. How does average relative humidity vary throughout the year and what is its relationship with apparent temperature?

SELECT
    year,
    month,
    AVG(apparent_temperature)        AS avg_heat_index,
    AVG(relative_humidity_2m)        AS avg_humidity,
    AVG(temperature_2m)              AS avg_temp_real,
    COUNT(*)                         AS n_horas
FROM meteo_enriched
WHERE apparent_temperature IS NOT NULL
  AND relative_humidity_2m IS NOT NULL
GROUP BY year, month
ORDER BY year, month;


-- 5. Which municipalities have the highest average concentrations of PM2.5 and PM10, the most dangerous pollutants for respiratory health?

SELECT
    municipio,
    departamento,
    msfl_code,

    AVG(med_concentracion_estandar) AS avg_contaminacion,
    MAX(med_concentracion_estandar) AS pico_contaminacion,
    COUNT(1) AS n_mediciones

FROM storeGov

WHERE msfl_code IN ('PM2.5', 'PM10')
  AND med_concentracion_estandar IS NOT NULL

GROUP BY
    municipio,
    departamento,
    msfl_code

ORDER BY avg_contaminacion DESC
LIMIT 20;