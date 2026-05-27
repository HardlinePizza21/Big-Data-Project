# Punto 8 — Visualización de Datos con Streamlit

## Descripción

Se desarrolló una aplicación web interactiva con **Streamlit** que visualiza los resultados de las 5 preguntas de negocio del proyecto. La aplicación lee los resultados directamente desde S3 (archivos Parquet generados por PySpark) y los presenta como gráficas y tablas interactivas.

## Infraestructura

La aplicación se desplegó en una instancia **EC2** (t2.medium, Ubuntu) expuesta en el puerto 8501:

- **IP pública**: `http://54.91.229.46:8501`
- **Puerto**: 8501 habilitado en el Security Group de la instancia
- **Librerías**: streamlit, pandas, boto3, matplotlib, pyarrow

## Instalación

```bash
sudo apt update -y
sudo apt install python3-pip -y
pip3 install streamlit pandas boto3 matplotlib pyarrow --break-system-packages
```

## Ejecución

```bash
python3 -m streamlit run ~/app.py --server.port 8501 --server.address 0.0.0.0
```

## Visualizaciones

La app presenta una sección por cada pregunta de negocio:

| Sección | Visualización |
|---------|--------------|
| Q1 | Gráfica de barras horizontales — Top 10 estaciones más calurosas |
| Q2 | Gráfica de barras — comparación de temperatura entre quincenas |
| Q3 | Gráfica de barras horizontales — Top 10 estaciones más lluviosas |
| Q4 | Gráfica de líneas doble eje — evolución mensual de temperatura y humedad |
| Q5 | Gráfica de barras agrupadas — PM2.5 vs PM10 por municipio |

Cada sección incluye además una tabla de datos interactiva con los valores numéricos.
