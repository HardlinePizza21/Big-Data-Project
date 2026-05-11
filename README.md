# Big-Data-Project
Te explico el proyecto en términos simples.

## ¿Qué te piden?

Es un proyecto de **Big Data en AWS** donde debes construir un pipeline completo de datos, desde la fuente hasta la visualización. La idea central es:

**Conseguir un dataset real → guardarlo en distintos lados → procesarlo en la nube → hacerle preguntas analíticas → mostrarlo visualmente**

---

## Los 8 puntos explicados

**1. Caso de estudio con preguntas de negocio**
Escoge un dataset (ej: datos climáticos, ventas, transporte) y plantea al menos 5 preguntas que quieras responder con esos datos. Ej: *"¿Cuál fue la ciudad más lluviosa en Colombia en 2024?"*

**2. Simular 3 fuentes de datos distintas**
Los mismos datos (o partes de ellos) deben venir de 3 lugares diferentes:
- Una **base de datos MariaDB** en AWS RDS
- **Archivos** (CSV, JSON, etc.) en una máquina EC2
- **URLs** públicas de internet

**3. Ingestar datos al Data Lake (S3)**
Mover automáticamente esos datos hacia AWS S3, organizados en zonas (raw = datos crudos, trusted = datos limpios).

**4. Limpiar y preparar los datos**
Usar **AWS Glue + Apache Spark** para transformar los datos de la zona raw a trusted (corregir tipos, eliminar nulos, estandarizar formatos, etc.)

**5. Catalogar las tablas**
Con **AWS Glue o Hive** crear un catálogo que diga "esta tabla existe, tiene estas columnas, está en este path de S3". Es como un diccionario de los datos.

**6. Consultas SQL analíticas**
Responder las preguntas del punto 1 usando **Athena, Hive o SparkSQL** (SQL sobre los datos en S3).

**7. Consultas analíticas con PySpark**
Responder las mismas preguntas (u otras) pero usando código Python con **PySpark** directamente.

**8. Visualización + API**
- Hacer gráficas (matplotlib, Grafana, PowerBI, etc.)
- Exponer algún resultado vía web con **Streamlit** publicado en API Gateway

---

## El flujo completo resumido

```
Fuentes (RDS / EC2 / URL)
        ↓
    S3 zona raw
        ↓  (Glue + Spark limpia)
   S3 zona trusted
        ↓  (Glue/Hive cataloga)
   Consultas SQL (Athena/Hive/SparkSQL)
   Consultas Python (PySpark)
        ↓
   Visualización + Streamlit
```

---

¿Quieres que te ayude a elegir un dataset concreto, formular las 5 preguntas de negocio, o estructurar el repositorio de GitHub?