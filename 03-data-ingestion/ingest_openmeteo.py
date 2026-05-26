"""
ingest_openmeteo.py
────────────────────────────────────────────────────────────────────────────────
Ingesta desde Open-Meteo Historical Weather + Air Quality APIs.
Lee las estaciones únicas del S3 (clean-data/*.parquet), consulta las APIs
por estación y rango de fechas, y escribe parquets particionados por
year/month/estacion_id en s3://smadrido/meteo-enriched/ listos para
Hive (Athena) y PySpark.

Diseñado para correr como AWS Glue Python Shell Job o localmente.

Variables disponibles alineadas con las preguntas de análisis:
  - apparent_temperature  → heat index / "feels like"
  - temperature_2m        → temperatura real
  - relative_humidity_2m  → humedad (necesaria para heat index)
  - wind_speed_10m        → viento (necesario para heat index)
  - precipitation         → lluvia (contexto de calidad de aire)
  - pm10, pm2_5, us_aqi   → partículas (complementan los datos MADS)
  - carbon_monoxide, nitrogen_dioxide, ozone, sulphur_dioxide

Responde preguntas como:
  - ¿Qué lugar tiene el mayor heat index o temperatura aparente?
  - ¿Es más caluroso en la primera o segunda quincena del mes?
  - ¿Cómo correlaciona la calidad del aire con la temperatura aparente?
────────────────────────────────────────────────────────────────────────────────
"""

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import requests
import io
import time
import logging
import random
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────────────────────────────────────

S3_BUCKET          = "smadrido"
S3_SOURCE_PREFIX   = "clean-data/"
S3_OUTPUT_PREFIX   = "meteo-enriched/"

# Rango de fechas a ingestar. None = detectar automáticamente desde los parquets.
DATE_START: Optional[date] = date(2020, 1, 1)
DATE_END:   Optional[date] = date(2024, 12, 31)

# Si DATE_START/END son None se usa el rango encontrado en los parquets menos
# un margen de solapamiento hacia atrás (para no dejar huecos en reingestas).
LOOKBACK_DAYS = 7

# Open-Meteo tiene un rate-limit generoso pero con cortesía usamos 2 hilos.
MAX_WORKERS = 2
# Pausa entre lotes de requests (segundos)
REQUEST_DELAY = 0.25

# Variables de la Historical Weather API
# https://archive-api.open-meteo.com/v1/archive
WEATHER_VARS = [
    "temperature_2m",
    "apparent_temperature",   # heat index / feels-like
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "weather_code",
]

# Variables de la Air Quality API
# https://air-quality-api.open-meteo.com/v1/air-quality
AQ_VARS = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "us_aqi",
]

WEATHER_API_URL  = "https://archive-api.open-meteo.com/v1/archive"
AQ_API_URL       = "https://air-quality-api.open-meteo.com/v1/air-quality"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Tipos de datos
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Station:
    estacion_id: int
    nombre_est: str
    latitud: float
    longitud: float
    altitud: float
    departamento: str
    municipio: str
    tipo_estacion: str
    date_min: date
    date_max: date


@dataclass
class MeteoRecord:
    """Una fila del parquet de salida."""
    estacion_id: int
    timestamp: datetime
    year: int
    month: int
    day: int
    # Weather
    temperature_2m: Optional[float]
    apparent_temperature: Optional[float]
    relative_humidity_2m: Optional[float]
    wind_speed_10m: Optional[float]
    precipitation: Optional[float]
    weather_code: Optional[int]
    # Air quality
    pm10: Optional[float]
    pm2_5: Optional[float]
    carbon_monoxide: Optional[float]
    nitrogen_dioxide: Optional[float]
    sulphur_dioxide: Optional[float]
    ozone: Optional[float]
    us_aqi: Optional[int]
    # Partition keys (redundantes pero necesarias para Hive)
    lat: float
    lon: float


# ──────────────────────────────────────────────────────────────────────────────
# Schema PyArrow (define orden de columnas y tipos exactos)
# ──────────────────────────────────────────────────────────────────────────────

OUTPUT_SCHEMA = pa.schema([
    pa.field("estacion_id",          pa.int32()),
    pa.field("timestamp",            pa.timestamp("ms")),
    pa.field("year",                 pa.int32()),
    pa.field("month",                pa.int32()),
    pa.field("day",                  pa.int32()),
    pa.field("temperature_2m",       pa.float32()),
    pa.field("apparent_temperature", pa.float32()),
    pa.field("relative_humidity_2m", pa.float32()),
    pa.field("wind_speed_10m",       pa.float32()),
    pa.field("precipitation",        pa.float32()),
    pa.field("weather_code",         pa.int16()),
    pa.field("pm10",                 pa.float32()),
    pa.field("pm2_5",                pa.float32()),
    pa.field("carbon_monoxide",      pa.float32()),
    pa.field("nitrogen_dioxide",     pa.float32()),
    pa.field("sulphur_dioxide",      pa.float32()),
    pa.field("ozone",                pa.float32()),
    pa.field("us_aqi",               pa.int16()),
    pa.field("lat",                  pa.float64()),
    pa.field("lon",                  pa.float64()),
])


# ──────────────────────────────────────────────────────────────────────────────
# Paso 1: leer estaciones únicas desde S3
# ──────────────────────────────────────────────────────────────────────────────

def list_parquet_files(s3: boto3.client, bucket: str, prefix: str) -> list[str]:
    """Lista todos los .parquet bajo el prefijo dado."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    log.info("Encontrados %d archivos parquet en s3://%s/%s", len(keys), bucket, prefix)
    return keys


def read_parquet_from_s3(s3: boto3.client, bucket: str, key: str) -> pa.Table:
    """Descarga un parquet de S3 y lo devuelve como PyArrow Table."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    return pq.read_table(buf)


def extract_stations(s3: boto3.client, bucket: str, prefix: str) -> list[Station]:
    """
    Lee una muestra de los parquets para extraer estaciones únicas con sus
    coordenadas y el rango de fechas disponible.
    """
    keys = list_parquet_files(s3, bucket, prefix)
    if not keys:
        raise RuntimeError(f"No se encontraron parquets en s3://{bucket}/{prefix}")

    station_rows: dict[int, dict] = {}

    for key in keys:
        try:
            table = read_parquet_from_s3(s3, bucket, key)
        except Exception as e:
            log.warning("No se pudo leer %s: %s", key, e)
            continue

        needed = {
            "estacion_id", "nombre_est", "latitud", "longitud", "altitud",
            "departamento", "municipio", "tipo_estacion",
            "med_fecha_inicio",
        }
        if not needed.issubset(set(table.column_names)):
            log.warning("Columnas faltantes en %s, saltando.", key)
            continue

        df = table.select(list(needed)).to_pydict()

        for i in range(len(df["estacion_id"])):
            eid = df["estacion_id"][i]
            if eid is None:
                continue
            eid = int(eid)
            ts = df["med_fecha_inicio"][i]
            row_date = ts.date() if isinstance(ts, datetime) else None

            if eid not in station_rows:
                station_rows[eid] = {
                    "estacion_id":  eid,
                    "nombre_est":   df["nombre_est"][i],
                    "latitud":      float(df["latitud"][i]),
                    "longitud":     float(df["longitud"][i]),
                    "altitud":      float(df["altitud"][i]) if df["altitud"][i] else 0.0,
                    "departamento": df["departamento"][i] or "",
                    "municipio":    df["municipio"][i] or "",
                    "tipo_estacion":df["tipo_estacion"][i] or "",
                    "date_min":     row_date,
                    "date_max":     row_date,
                }
            else:
                if row_date:
                    if station_rows[eid]["date_min"] is None or row_date < station_rows[eid]["date_min"]:
                        station_rows[eid]["date_min"] = row_date
                    if station_rows[eid]["date_max"] is None or row_date > station_rows[eid]["date_max"]:
                        station_rows[eid]["date_max"] = row_date

    stations = [Station(**r) for r in station_rows.values()
                if r["date_min"] is not None]
    log.info("Extraídas %d estaciones únicas.", len(stations))
    return stations


# ──────────────────────────────────────────────────────────────────────────────
# Paso 2: llamadas a Open-Meteo
# ──────────────────────────────────────────────────────────────────────────────

def fetch_weather(lat: float, lon: float, start: date, end: date) -> dict:
    """
    Llama a la Historical Weather API de Open-Meteo.
    Devuelve el JSON completo o lanza excepción.
    """
    params = {
        "latitude":       lat,
        "longitude":      lon,
        "start_date":     start.isoformat(),
        "end_date":       end.isoformat(),
        "hourly":         ",".join(WEATHER_VARS),
        "timezone":       "America/Bogota",
        "wind_speed_unit":"ms",
    }
    r = requests.get(WEATHER_API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_air_quality(lat: float, lon: float, start: date, end: date) -> dict:
    """
    Llama a la Air Quality API de Open-Meteo.
    Devuelve el JSON completo o lanza excepción.
    """
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "hourly":     ",".join(AQ_VARS),
        "timezone":   "America/Bogota",
    }
    r = requests.get(AQ_API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_hourly(weather_json: dict, aq_json: dict, station: Station) -> list[dict]:
    """
    Combina las respuestas de ambas APIs en una lista de dicts (una fila por hora).
    """
    wh = weather_json.get("hourly", {})
    ah = aq_json.get("hourly", {})

    times_w = wh.get("time", [])
    times_a = ah.get("time", [])

    # Indexar AQ por timestamp para un join rápido
    aq_index: dict[str, dict] = {}
    for i, t in enumerate(times_a):
        aq_index[t] = {v: ah[v][i] if ah.get(v) else None for v in AQ_VARS}

    rows = []
    for i, ts_str in enumerate(times_w):
        ts = datetime.fromisoformat(ts_str)
        aq = aq_index.get(ts_str, {v: None for v in AQ_VARS})

        rows.append({
            "estacion_id":          station.estacion_id,
            "timestamp":            ts,
            "year":                 ts.year,
            "month":                ts.month,
            "day":                  ts.day,
            "temperature_2m":       _f32(wh.get("temperature_2m", [None])[i]),
            "apparent_temperature": _f32(wh.get("apparent_temperature", [None])[i]),
            "relative_humidity_2m": _f32(wh.get("relative_humidity_2m", [None])[i]),
            "wind_speed_10m":       _f32(wh.get("wind_speed_10m", [None])[i]),
            "precipitation":        _f32(wh.get("precipitation", [None])[i]),
            "weather_code":         _i16(wh.get("weather_code", [None])[i]),
            "pm10":                 _f32(aq.get("pm10")),
            "pm2_5":                _f32(aq.get("pm2_5")),
            "carbon_monoxide":      _f32(aq.get("carbon_monoxide")),
            "nitrogen_dioxide":     _f32(aq.get("nitrogen_dioxide")),
            "sulphur_dioxide":      _f32(aq.get("sulphur_dioxide")),
            "ozone":                _f32(aq.get("ozone")),
            "us_aqi":               _i16(aq.get("us_aqi")),
            "lat":                  station.latitud,
            "lon":                  station.longitud,
        })
    return rows


def _f32(v) -> Optional[float]:
    return float(v) if v is not None else None


def _i16(v) -> Optional[int]:
    return int(v) if v is not None else None


def fetch_station(station: Station, start: date, end: date, max_retries: int = 5) -> list[dict]:
    """Orquesta la descarga de weather + AQ con retry ante 429."""
    for attempt in range(max_retries):
        try:
            w_json = fetch_weather(station.latitud, station.longitud, start, end)
            time.sleep(REQUEST_DELAY)
            a_json = fetch_air_quality(station.latitud, station.longitud, start, end)
            rows = parse_hourly(w_json, a_json, station)
            log.info(
                "Estación %d (%s): %d registros horarios.",
                station.estacion_id, station.nombre_est, len(rows),
            )
            return rows

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Backoff exponencial: 60s, 120s, 240s, 480s, 960s
                wait = 60 * (2 ** attempt) + random.uniform(0, 5)
                log.warning(
                    "429 en estación %d (intento %d/%d). Esperando %.0fs...",
                    station.estacion_id, attempt + 1, max_retries, wait,
                )
                time.sleep(wait)
            else:
                log.error("HTTP error en estación %d: %s", station.estacion_id, e)
                return []  # Error no recuperable

        except Exception as e:
            log.error("Error en estación %d: %s", station.estacion_id, e)
            return []

    log.error("Estación %d agotó los %d reintentos. Chunk saltado.", station.estacion_id, max_retries)
    return []  # Retorna vacío → process_station_streaming no escribe nada

def fetch_station_chunked(station: Station, start: date, end: date, chunk_months: int = 3) -> list[dict]:
    """Divide el rango en chunks para no saturar la API ni hacer timeouts."""
    all_rows = []
    cursor = start
    while cursor <= end:
        chunk_end = min(
            (cursor + relativedelta(months=chunk_months) - timedelta(days=1)),
            end
        )
        log.info("  Chunk %s → %s para estación %d", cursor, chunk_end, station.estacion_id)
        rows = fetch_station(station, cursor, chunk_end)
        all_rows.extend(rows)
        time.sleep(REQUEST_DELAY)
        cursor = chunk_end + timedelta(days=1)
    return all_rows

# ──────────────────────────────────────────────────────────────────────────────
# Paso 3: escribir parquets particionados en S3
# ──────────────────────────────────────────────────────────────────────────────

def rows_to_table(rows: list[dict]) -> pa.Table:
    """Convierte lista de dicts al schema de salida."""
    if not rows:
        return OUTPUT_SCHEMA.empty_table()

    cols = {f.name: [] for f in OUTPUT_SCHEMA}
    for row in rows:
        for fname in cols:
            cols[fname].append(row.get(fname))

    arrays = []
    for f in OUTPUT_SCHEMA:
        try:
            arrays.append(pa.array(cols[f.name], type=f.type))
        except Exception:
            # Si hay problemas de tipo, caer a null array
            arrays.append(pa.array([None] * len(rows), type=f.type))

    return pa.table(dict(zip([f.name for f in OUTPUT_SCHEMA], arrays)),
                    schema=OUTPUT_SCHEMA)


def write_partition(
    s3: boto3.client,
    table: pa.Table,
    bucket: str,
    prefix: str,
    year: int,
    month: int,
    estacion_id: int,
) -> None:
    """
    Escribe un parquet en:
    s3://{bucket}/{prefix}year={year}/month={month:02}/estacion_id={estacion_id}/data.parquet
    """
    key = (
        f"{prefix}"
        f"year={year}/month={month:02d}/"
        f"estacion_id={estacion_id}/data.parquet"
    )
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        write_statistics=True,
    )
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info(
        "Escrito s3://%s/%s (%d filas, %.1f KB)",
        bucket, key, len(table), buf.tell() / 1024,
    )


def write_station_data(
    s3: boto3.client,
    rows: list[dict],
    station: Station,
    bucket: str,
    prefix: str,
) -> None:
    """Particiona los rows por year/month y escribe un parquet por partición."""
    if not rows:
        return

    # Agrupar por (year, month)
    partitions: dict[tuple[int, int], list[dict]] = {}
    for row in rows:
        key = (row["year"], row["month"])
        partitions.setdefault(key, []).append(row)

    for (year, month), part_rows in sorted(partitions.items()):
        if already_ingested(s3, bucket, prefix, station.estacion_id, year, month):
            log.info("Ya existe year=%d month=%02d estación %d, saltando.", year, month, station.estacion_id)
            continue
        table = rows_to_table(part_rows)
        write_partition(
            s3, table, bucket, prefix,
            year, month, station.estacion_id,
        )

def already_ingested(s3, bucket: str, prefix: str, estacion_id: int, year: int, month: int) -> bool:
    key = f"{prefix}year={year}/month={month:02d}/estacion_id={estacion_id}/data.parquet"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def process_station_streaming(
    s3,
    station: Station,
    start: date,
    end: date,
    bucket: str,
    prefix: str,
    chunk_months: int = 3,
) -> None:
    cursor = start
    while cursor <= end:
        chunk_end = min(
            cursor + relativedelta(months=chunk_months) - timedelta(days=1),
            end,
        )

        # Determinar qué meses cubre este chunk
        partitions_in_chunk = []
        temp = cursor
        while temp <= chunk_end:
            partitions_in_chunk.append((temp.year, temp.month))
            temp = (temp.replace(day=1) + relativedelta(months=1))

        # Si TODOS los meses del chunk ya existen en S3, saltar sin llamar a la API
        if all(
            already_ingested(s3, bucket, prefix, station.estacion_id, y, m)
            for y, m in partitions_in_chunk
        ):
            log.info(
                "  Chunk %s → %s ya está completo en S3, saltando.",
                cursor, chunk_end,
            )
            cursor = chunk_end + timedelta(days=1)
            continue

        # Solo si falta algo, llamar a la API
        log.info("  Chunk %s → %s para estación %d", cursor, chunk_end, station.estacion_id)
        rows = fetch_station(station, cursor, chunk_end)

        if rows:
            partitions: dict[tuple, list] = {}
            for row in rows:
                partitions.setdefault((row["year"], row["month"]), []).append(row)

            for (year, month), part_rows in sorted(partitions.items()):
                if already_ingested(s3, bucket, prefix, station.estacion_id, year, month):
                    log.info("  Ya existe year=%d month=%02d estación %d, saltando.", year, month, station.estacion_id)
                    continue
                table = rows_to_table(part_rows)
                write_partition(s3, table, bucket, prefix, year, month, station.estacion_id)

        del rows
        time.sleep(REQUEST_DELAY)
        cursor = chunk_end + timedelta(days=1)

# ──────────────────────────────────────────────────────────────────────────────
# Orquestador principal
# ──────────────────────────────────────────────────────────────────────────────

def resolve_date_range(
    stations: list[Station],
    date_start: Optional[date],
    date_end: Optional[date],
) -> tuple[date, date]:
    """
    Si no se especificaron fechas, usa el rango de los parquets del gobierno
    menos LOOKBACK_DAYS de margen para evitar huecos en reingestas.
    Open-Meteo solo da datos hasta ayer.
    """
    yesterday = date.today() - timedelta(days=1)

    if date_start and date_end:
        return date_start, min(date_end, yesterday)

    min_dates = [s.date_min for s in stations if s.date_min]
    max_dates = [s.date_max for s in stations if s.date_max]

    if not min_dates:
        # Fallback: último mes
        start = date.today().replace(day=1) - timedelta(days=1)
        start = start.replace(day=1)
        return start, yesterday

    global_min = min(min_dates)
    global_max = min(max(max_dates), yesterday)

    if date_start:
        start = date_start
    else:
        # Reingesta desde (max conocido - lookback) para no dejar huecos
        start = global_max - timedelta(days=LOOKBACK_DAYS)
        start = max(start, global_min)

    end = date_end if date_end else global_max
    end = min(end, yesterday)

    log.info("Rango de fechas resuelto: %s → %s", start.isoformat(), end.isoformat())
    return start, end


def run() -> None:
    log.info("Iniciando ingesta Open-Meteo → S3")
    s3 = boto3.client("s3")

    # 1. Extraer estaciones
    stations = extract_stations(s3, S3_BUCKET, S3_SOURCE_PREFIX)
    if not stations:
        log.error("No se encontraron estaciones. Abortando.")
        return

    # 2. Resolver rango de fechas
    start, end = resolve_date_range(stations, DATE_START, DATE_END)

    # 3. Fetch paralelo por estación
    log.info(
        "Descargando datos para %d estaciones del %s al %s...",
        len(stations), start, end,
    )

    for i, st in enumerate(stations, 1):
        log.info("── [%d/%d] Estación %d (%s) ──", i, len(stations), st.estacion_id, st.nombre_est)
        process_station_streaming(s3, st, start, end, S3_BUCKET, S3_OUTPUT_PREFIX)

    log.info("Ingesta completada. Total estaciones procesadas: esperemos que todas.")

if __name__ == "__main__":
    run()
