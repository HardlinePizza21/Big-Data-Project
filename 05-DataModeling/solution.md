Once the government dataset has been cleaned and the Open-Meteo data has been successfully uploaded to the S3 bucket, we proceed with the creation of the Hive tables using the script [tableCreation.sql](./tableCreation.sql).

Inside this script, two different data models are defined. The first model corresponds to the government dataset, which required additional preprocessing due to inconsistencies caused by comma-separated text fields and formatting issues in the original source data.

The resulting schema is structured as follows:

# storeGov

| Nombre de la columna        | Descripción                                                      | Nombre del campo API | Tipo de Dato |
|-----------------------------|------------------------------------------------------------------|----------------------|---------------|
| estacion_id                 | Identificador único de la estación                               | estacion_id          | INT           |
| nombre_fgda                 | Nombre de la entidad o fuente gubernamental                      | nombre_fgda          | STRING        |
| nombre_est                  | Nombre de la estación de monitoreo                               | nombre_est           | STRING        |
| msfl_code                   | Código MSFL asociado a la estación                               | msfl_code            | STRING        |
| med_concentracion_estandar  | Concentración promedio estandarizada medida                      | med_concentracion    | DOUBLE        |
| med_fecha_inicio            | Fecha y hora de inicio de la medición                            | med_fecha_inicio     | TIMESTAMP     |
| med_fecha_final             | Fecha y hora final de la medición                                | med_fecha_final      | TIMESTAMP     |
| latitud                     | Latitud geográfica de la estación                                | latitud              | DOUBLE        |
| longitud                    | Longitud geográfica de la estación                               | longitud             | DOUBLE        |
| altitud                     | Altitud de la estación sobre el nivel del mar                    | altitud              | DOUBLE        |
| nombre_unidad               | Nombre de la unidad de medida                                    | nombre_unidad        | STRING        |
| sigla_unidad                | Abreviatura de la unidad de medida                               | sigla_unidad         | STRING        |
| duracion                    | Duración de la medición                                          | duracion             | INT           |
| codigo_departamento         | Código del departamento                                          | codigo_departamento  | INT           |
| departamento                | Nombre del departamento                                          | departamento         | STRING        |
| codigo_municipio            | Código del municipio                                             | codigo_municipio     | STRING        |
| municipio                   | Nombre del municipio                                             | municipio            | STRING        |
| tipo_estacion               | Tipo de estación de monitoreo                                    | tipo_estacion        | STRING        |


An important improvement is that each column was assigned a more accurate computational data type. In the official government dataset documentation, numeric fields were not clearly differentiated between integers and floating-point values, so additional preprocessing and type inference were required.

Additionally, the original date format was not SQL-native, which made it incompatible with Hive and Athena queries. To solve this, all date fields were transformed into a standard SQL TIMESTAMP format during the cleaning process.

# meteo_enriched

| Nombre de la columna        | Descripción                                                      | Nombre del campo API           | Tipo de Dato |
|-----------------------------|------------------------------------------------------------------|--------------------------------|---------------|
| estacion_id                 | Identificador único de la estación                               | estacion_id                    | INT           |
| timestamp                   | Fecha y hora del registro meteorológico                          | time                           | TIMESTAMP     |
| day                         | Día del mes correspondiente al registro                          | derived from timestamp         | INT           |
| temperature_2m              | Temperatura del aire a 2 metros de altura                        | temperature_2m                 | FLOAT         |
| apparent_temperature        | Temperatura aparente percibida                                   | apparent_temperature           | FLOAT         |
| relative_humidity_2m        | Humedad relativa a 2 metros                                      | relative_humidity_2m           | FLOAT         |
| wind_speed_10m              | Velocidad del viento a 10 metros                                 | wind_speed_10m                 | FLOAT         |
| precipitation               | Cantidad de precipitación registrada                             | precipitation                  | FLOAT         |
| weather_code                | Código meteorológico de condiciones climáticas                   | weather_code                   | SMALLINT      |
| pm10                        | Concentración de partículas PM10                                 | pm10                           | FLOAT         |
| pm2_5                       | Concentración de partículas PM2.5                                | pm2_5                          | FLOAT         |
| carbon_monoxide             | Concentración de monóxido de carbono                             | carbon_monoxide                | FLOAT         |
| nitrogen_dioxide            | Concentración de dióxido de nitrógeno                            | nitrogen_dioxide               | FLOAT         |
| sulphur_dioxide             | Concentración de dióxido de azufre                               | sulphur_dioxide                | FLOAT         |
| ozone                       | Concentración de ozono                                           | ozone                          | FLOAT         |
| us_aqi                      | Índice de calidad del aire de Estados Unidos                     | us_aqi                         | SMALLINT      |
| lat                         | Latitud utilizada en la consulta de Open-Meteo                   | latitude                       | DOUBLE        |
| lon                         | Longitud utilizada en la consulta de Open-Meteo                  | longitude                      | DOUBLE        |
| year                        | Año de la partición Hive                                         | derived partition              | INT           |
| month                       | Mes de la partición Hive                                         | derived partition              | INT           |

Although the Open-Meteo API schema is much clearer and better structured than the government dataset, which made it significantly easier for us to define and implement a consistent schema for our data pipeline.