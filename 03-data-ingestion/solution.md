To ingest data from the Open-Meteo API, we created a script that, based on the coordinates of the stations (latitude and longitude), requests specific data that is missing from the government dataset and allows us to answer additional business questions. The script used for this process is [ingest_openmeteo](./ingest_openmeteo.py).

This script was executed from an EC2 instance, where we experienced some RAM-related issues due to the instance limitations. Nevertheless, we successfully uploaded the processed data to `s3://smadrido/meteo-enriched/`.

<div style="text-align: center;">
	<img src="../images/OpenMeteoStorage.png" width="500">
</div>

In addition, the data is stored using a **Hive partitioning** strategy, which provides a more efficient storage and querying mechanism.

The script queries two different endpoints: the historical air quality API and the historical weather API. It then combines the results, formats the data, and manages the download process to avoid overloading the API and to recover downloads in case of failures.

Finally, the information is indexed using the following structure:

```s3://{bucket}/{prefix}year={year}/month={month:02}/estacion_id={estacion_id}/data.parquet```


The following table details all the columns included in the Open-Meteo enriched data:

| Nombre de la columna       | Descripción                                                        | Nombre del campo API     | Tipo de Dato |
|----------------------------|--------------------------------------------------------------------|--------------------------|---------------|
| TIMESTAMP                  | Marca de tiempo UNIX de la medición meteorológica                 | timestamp                | Número entero |
| DAY                        | Día del mes de la medición                                        | day                      | Número entero |
| TEMPERATURE_2M             | Temperatura del aire a 2 metros sobre la superficie               | temperature_2m           | Número decimal |
| APPARENT_TEMPERATURE       | Temperatura aparente o sensación térmica                          | apparent_temperature     | Número decimal |
| RELATIVE_HUMIDITY_2M       | Humedad relativa del aire a 2 metros                              | relative_humidity_2m     | Número decimal |
| WIND_SPEED_10M             | Velocidad del viento a 10 metros                                  | wind_speed_10m           | Número decimal |
| PRECIPITATION              | Precipitación acumulada                                           | precipitation            | Número decimal |
| WEATHER_CODE               | Código meteorológico que describe las condiciones del clima       | weather_code             | Número entero |
| PM10                       | Concentración de partículas PM10                                  | pm10                     | Número decimal |
| PM2_5                      | Concentración de partículas PM2.5                                 | pm2_5                    | Número decimal |
| CARBON_MONOXIDE            | Concentración de monóxido de carbono                              | carbon_monoxide          | Número decimal |
| NITROGEN_DIOXIDE           | Concentración de dióxido de nitrógeno                             | nitrogen_dioxide         | Número decimal |
| SULPHUR_DIOXIDE            | Concentración de dióxido de azufre                                | sulphur_dioxide          | Número decimal |
| OZONE                      | Concentración de ozono                                            | ozone                    | Número decimal |
| US_AQI                     | Índice de calidad del aire basado en el estándar de Estados Unidos | us_aqi                   | Número entero |
| LAT                        | Latitud geográfica de la medición                                 | lat                      | Número decimal |
| LON                        | Longitud geográfica de la medición                                | lon                      | Número decimal |
| YEAR                       | Año de la medición (partición)                                    | year                     | Número entero |
| MONTH                      | Mes de la medición (partición)                                    | month                    | Texto |
| ESTACION_ID                | Identificador de la estación meteorológica (partición)            | estacion_id              | Número entero |