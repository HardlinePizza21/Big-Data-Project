
-- ─────────────────────────────────────────────────────────────────────────────
-- Q1: ¿Qué estación tiene el mayor heat index (apparent_temperature) promedio?
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    estacion_id,
    AVG(apparent_temperature)                   AS avg_heat_index,
    MAX(apparent_temperature)                   AS max_heat_index,
    PERCENTILE_APPROX(apparent_temperature, 0.9) AS p90_heat_index,
    COUNT(*)                                    AS n_horas
FROM meteo_enriched
WHERE year BETWEEN 2022 AND 2024
  AND apparent_temperature IS NOT NULL
GROUP BY estacion_id
ORDER BY avg_heat_index DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2: ¿Es más caluroso en la primera o segunda quincena del mes?
-- Comparación global + por estación
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    estacion_id,
    CASE WHEN day <= 15 THEN '1ra_quincena' ELSE '2da_quincena' END AS quincena,
    AVG(apparent_temperature)                    AS avg_apparent_temp,
    AVG(temperature_2m)                          AS avg_temp_real,
    STDDEV_POP(apparent_temperature)             AS std_apparent_temp,
    COUNT(*)                                     AS n_horas
FROM meteo_enriched
WHERE year BETWEEN 2022 AND 2024
  AND apparent_temperature IS NOT NULL
GROUP BY
    estacion_id,
    CASE WHEN day <= 15 THEN '1ra_quincena' ELSE '2da_quincena' END
ORDER BY estacion_id, quincena;

-- Resumen global (sin agrupar por estación)
SELECT
    CASE WHEN day <= 15 THEN '1ra_quincena' ELSE '2da_quincena' END AS quincena,
    AVG(apparent_temperature)   AS avg_heat_index,
    AVG(temperature_2m)         AS avg_temp_real,
    COUNT(*)                    AS n_horas
FROM meteo_enriched
WHERE year BETWEEN 2022 AND 2024
  AND apparent_temperature IS NOT NULL
GROUP BY CASE WHEN day <= 15 THEN '1ra_quincena' ELSE '2da_quincena' END;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3: Ranking de estaciones por AQI promedio (calidad del aire)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    estacion_id,
    AVG(us_aqi)  AS avg_aqi,
    MAX(us_aqi)  AS max_aqi,
    AVG(pm2_5)   AS avg_pm25,
    AVG(pm10)    AS avg_pm10
FROM meteo_enriched
WHERE year BETWEEN 2022 AND 2024
  AND us_aqi IS NOT NULL
GROUP BY estacion_id
ORDER BY avg_aqi DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4: Correlación mensual entre temperatura aparente y AQI
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    year,
    month,
    AVG(apparent_temperature) AS avg_heat_index,
    AVG(us_aqi)               AS avg_aqi,
    AVG(pm2_5)                AS avg_pm25
FROM meteo_enriched
WHERE apparent_temperature IS NOT NULL
  AND us_aqi IS NOT NULL
GROUP BY year, month
ORDER BY year, month;
