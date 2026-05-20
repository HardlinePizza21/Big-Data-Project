CREATE EXTERNAL TABLE storeGov (
  ESTACION_ID               INT,
  NOMBRE_FGDA               STRING,
  NOMBRE_EST                STRING,
  MSFL_CODE                 STRING,
  MED_CONCENTRACION_ESTANDAR DOUBLE,
  MED_FECHA_INICIO          STRING,
  MED_FECHA_FINAL           STRING,
  LATITUD                   DOUBLE,
  LONGITUD                  DOUBLE,
  ALTITUD                   INT,
  NOMBRE_UNIDAD             STRING,
  SIGLA_UNIDAD              STRING,
  DURACION                  INT,
  CODIGO_DEPARTAMENTO       INT,
  DEPARTAMENTO              STRING,
  CODIGO_MUNICIPIO          INT,
  MUNICIPIO                 STRING,
  TIPO_ESTACION             STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://smadrido/store/';