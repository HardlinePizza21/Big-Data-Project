For our business, we have chosen air pollution in Colombia. We have two data sources:

The first source is [Open-Meteo Air Quality](https://open-meteo.com/en/docs/air-quality-api) Air Quality, and the second one is ["Calidad del aire en Colombia"](https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Calidad-del-Aire-en-Colombia/g4t8-zkc3/about_data) a dataset distributed by the Colombian government.

Both sources will be downloaded. Open-Meteo specializes in updated real-time data, while the second source, IDEAM air quality data, contains historical Colombian records. Open-Meteo data will be continuously fetched by an EC2 instance and saved into a file simulating an I/O device, while the other dataset will be stored in an RDS database and later moved automatically to AWS S3.

The IDEAM dataset looks like this

| Nombre de la columna           | Descripción                                                                 | Nombre del campo API         | Tipo de Dato              |
|--------------------------------|-----------------------------------------------------------------------------|------------------------------|---------------------------|
| ESTACION_ID                    | Identificación de la estación                                              | estacion_id                 | Texto                     |
| NOMBRE_FGDA                    | Nombre de la autoridad ambiental                                           | nombre_fgda                 | Texto                     |
| NOMBRE_EST                     | Nombre de la estación                                                      | nombre_est                  | Texto                     |
| MSFL_CODE                      | Nombre del contaminante atmosférico                                        | msfl_code                   | Texto                     |
| MED_CONCENTRACION_ESTANDAR     | Valor de la concentración del contaminante atmosférico medido por la estación | med_concentracion_estandar | Número                    |
| MED_FECHA_INICIO               | Fecha y hora inicio de la medición                                         | med_fecha_inicio            | Marca de tiempo variable  |
| MED_FECHA_FINAL                | Fecha y hora final de la medición                                          | med_fecha_final             | Marca de tiempo variable  |
| LATITUD                        | Coordenada latitudinal de la estación en grados decimales                  | latitud                     | Texto                     |
| LONGITUD                       | Coordenada longitudinal de la estación en grados decimales                 | longitud                    | Texto                     |
| ALTITUD                        | Altitud sobre el nivel del mar en metros                                   | altitud                     | Número                    |
| NOMBRE_UNIDAD                  | Nombre de la unidad de medida de la variable                               | nombre_unidad               | Texto                     |
| SIGLA_UNIDAD                   | Sigla con la que se denomina a la unidad de medida de la variable          | sigla_unidad                | Texto                     |
| DURACIÓN                       | Duración del monitoreo en minutos                                          | duraci_n                    | Número                    |
| CODIGO_DEPARTAMENTO            | Código DANE del departamento                                               | codigo_departamento         | Número                    |
| DEPARTAMENTO                   | Nombre del departamento                                                    | departamento                | Texto                     |
| CODIGO_MUNICIPIO               | Código DANE del municipio                                                  | codigo_municipio            | Texto                     |
| MUNICIPIO                      | Nombre del municipio                                                       | municipio                   | Texto                     |
| TIPO_ESTACION                  | Clasificación de la estación de acuerdo con la frecuencia de medición      | tipo_estacion               | Texto                     |


and Open-Api looks like this

```
{
    "latitude": 52.52,
    "longitude": 13.419,
    "elevation": 44.812,
    "generationtime_ms": 2.2119,
    "utc_offset_seconds": 0,
    "timezone": "Europe/Berlin",
    "timezone_abbreviation": "CEST",
    "hourly": {
        "time": ["2022-07-01T00:00", "2022-07-01T01:00", "2022-07-01T02:00", ...],
        "temperature_2m": [13, 12.7, 12.7, 12.5, 12.5, 12.8, 13, 12.9, 13.3, ...]
    },
    "hourly_units": {
        "temperature_2m": "°C"
    }
}
```
So, we have developed the following business questions:

- Which department has the highest number of stations? (Government dataset)
- Is it usually hotter during the first 15 days of the month or during the last 15 days of the month? (Open-Meteo)
- Which place has the highest heat index or apparent temperature? (Crossed data)


//ELIMINAR
TODO: 
RH = relative humidity
T = temperature
Heat Index = -42.379 + (2.04901523*T) + (10.14333127*RH) - (.22475541*T*RH) - (.00683783*T*T) - (.05481717*RH*RH) + (.00122874*T*T*RH) + (.00085282*T*RH*RH) - (.00000199*T*T*RH*RH)

Easy way 
Si no quieres usar ecuaciones complejas, puedes calcular un estimado rápido en grados Celsius usando una regla práctica:Asegúrate de que la temperatura real supere los \(26^{\circ }\text{C}\).A la humedad relativa en porcentaje, réstale \(40\) y divide el resultado entre \(10\).Súmale este valor directamente a la temperatura real.
