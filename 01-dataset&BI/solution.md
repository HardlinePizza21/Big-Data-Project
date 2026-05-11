For our business, we have chosen air pollution in Colombia. We have two data sources:

The first source is [Open-Meteo Air Quality](https://open-meteo.com/en/docs/air-quality-api) Air Quality, and the second one is ["Calidad del aire en Colombia"](https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Calidad-del-Aire-en-Colombia/g4t8-zkc3/about_data) a dataset distributed by the Colombian government.

Both sources will be downloaded. Open-Meteo specializes in updated real-time data, while the second source, IDEAM air quality data, contains historical Colombian records. Open-Meteo data will be continuously fetched by an EC2 instance and saved into a file simulating an I/O device, while the other dataset will be stored in an RDS database and later moved automatically to AWS S3.