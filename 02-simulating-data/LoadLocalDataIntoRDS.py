# pip install mysql-connector-python pandas tqdm

import mysql.connector
import pandas as pd
from tqdm import tqdm
import os
import json
import time
import math
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ── Configuración ──────────────────────────────────────────
CSV_PATH = "/home/ano/Downloads/Calidad_del_Aire_en_Colombia_20260516.csv"
CHECKPOINT_PATH = "checkpoint.json"
DB_CONFIG = {
    "host": "bigdata-proyecto.c9k3e9wvgt2e.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "user": "admin",
    "password": "samuel14madrid",
    "ssl_disabled": False,
    "ssl_ca": "./global-bundle.pem",
    "autocommit": False,
}
CHUNK_SIZE = 50_000
# ───────────────────────────────────────────────────────────

# Columnas esperadas con su tipo de destino
COLUMNAS_SCHEMA = {
    "estacion_id":                 "str",
    "nombre_fgda":                 "str",
    "nombre_est":                  "str",
    "msfl_code":                   "str",
    "med_concentracion_estandar":  "decimal",
    "med_fecha_inicio":            "datetime",
    "med_fecha_final":             "datetime",
    "latitud":                     "decimal",
    "longitud":                    "decimal",
    "altitud":                     "decimal",
    "nombre_unidad":               "str",
    "sigla_unidad":                "str",
    "duracion":                    "int",
    "codigo_departamento":         "str",
    "departamento":                "str",
    "codigo_municipio":            "str",
    "municipio":                   "str",
    "tipo_estacion":               "str",
}

# Alias de columnas que pueden venir con nombre distinto en el CSV
ALIAS_COLUMNAS = {
    "duraci_n":  "duracion",
    "duración":  "duracion",
    "duration":  "duracion",
}


# ── Conexión ──────────────────────────────────────────────
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


# ── Checkpoint ────────────────────────────────────────────
def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            data = json.load(f)
            print(f"♻️  Reanudando desde chunk {data['chunk_index']} "
                  f"({data['filas_insertadas']:,} filas ya insertadas)")
            return data
    return {"chunk_index": 0, "filas_insertadas": 0}


def save_checkpoint(chunk_index, filas_insertadas, filas_descartadas):
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({
            "chunk_index": chunk_index,
            "filas_insertadas": filas_insertadas,
            "filas_descartadas": filas_descartadas,
        }, f)


# ── DB setup ──────────────────────────────────────────────
def setup_database(conn):
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS calidad_aire")
    cur.execute("USE calidad_aire")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mediciones (
            id                         INT AUTO_INCREMENT PRIMARY KEY,
            estacion_id                VARCHAR(20),
            nombre_fgda                VARCHAR(100),
            nombre_est                 VARCHAR(100),
            msfl_code                  VARCHAR(50),
            med_concentracion_estandar DECIMAL(15,4),
            med_fecha_inicio           DATETIME,
            med_fecha_final            DATETIME,
            latitud                    DECIMAL(12,6),
            longitud                   DECIMAL(12,6),
            altitud                    DECIMAL(10,2),
            nombre_unidad              VARCHAR(50),
            sigla_unidad               VARCHAR(20),
            duracion                   INT,
            codigo_departamento        VARCHAR(10),
            departamento               VARCHAR(100),
            codigo_municipio           VARCHAR(10),
            municipio                  VARCHAR(100),
            tipo_estacion              VARCHAR(50)
        )
    """)
    cur.execute("SET foreign_key_checks = 0")
    cur.execute("SET unique_checks = 0")
    conn.commit()
    cur.close()
    print("✅ Base de datos y tabla listas\n")


# ── Limpieza de tipos ─────────────────────────────────────
def limpiar_str(series: pd.Series, max_len: int = None) -> pd.Series:
    """Convierte a str, recorta espacios, vacíos → None."""
    s = series.astype(str).str.strip()
    s = s.replace({"nan": None, "None": None, "": None, "NaN": None, "NULL": None})
    if max_len:
        s = s.str[:max_len]
    return s


def limpiar_decimal(series: pd.Series) -> pd.Series:
    """Convierte a numérico; comas → puntos; fuera de rango → None."""
    s = series.astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)   # 3,14 → 3.14
    s = s.str.replace(r"[^\d.\-]", "", regex=True)  # elimina caracteres raros
    s = pd.to_numeric(s, errors="coerce")
    return s


def limpiar_int(series: pd.Series) -> pd.Series:
    """Convierte a entero; texto → None."""
    s = limpiar_decimal(series)
    # Redondear antes de convertir a Int64 (nullable integer)
    s = s.round(0).astype("Int64")
    return s


FORMATOS_FECHA = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
]


def limpiar_datetime(series: pd.Series) -> pd.Series:
    """Intenta parsear fechas con múltiples formatos; inválidas → None."""
    s = series.astype(str).str.strip()
    s = s.replace({"nan": None, "None": None, "": None, "NaN": None})

    resultado = pd.to_datetime(s, errors="coerce", format="mixed", dayfirst=False)

    # Si hay muchos NaT, intentar formatos explícitos uno a uno
    pct_nat = resultado.isna().mean()
    if pct_nat > 0.05:
        for fmt in FORMATOS_FECHA:
            parsed = pd.to_datetime(s, errors="coerce", format=fmt)
            # Solo rellenar donde el anterior falló
            mask = resultado.isna() & parsed.notna()
            resultado = resultado.where(~mask, parsed)

    return resultado.dt.strftime("%Y-%m-%d %H:%M:%S").where(resultado.notna(), None)


def limpiar_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Limpia y castea todas las columnas del chunk.
    Devuelve (df_limpio, n_filas_descartadas).
    Las filas con fecha_inicio inválida se descartan (son inutilizables).
    Las demás columnas con valores inválidos se dejan como NULL.
    """
    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns=ALIAS_COLUMNAS)
    # Eliminar columnas sin nombre útil
    df = df[[c for c in df.columns if not c.startswith((":", "unnamed"))]]

    # Aplicar limpieza por tipo
    for col, tipo in COLUMNAS_SCHEMA.items():
        if col not in df.columns:
            df[col] = None
            continue
        if tipo == "str":
            df[col] = limpiar_str(df[col])
        elif tipo == "decimal":
            df[col] = limpiar_decimal(df[col])
        elif tipo == "int":
            df[col] = limpiar_int(df[col])
        elif tipo == "datetime":
            df[col] = limpiar_datetime(df[col])

    # Descartar filas donde med_fecha_inicio es inválida (dato incompleto crítico)
    filas_antes = len(df)
    df = df[df["med_fecha_inicio"].notna()]
    filas_descartadas = filas_antes - len(df)

    # Conservar solo columnas del schema
    columnas_presentes = [c for c in COLUMNAS_SCHEMA if c in df.columns]
    df = df[columnas_presentes]

    # Reemplazar NaN/NaT por None para MySQL
    df = df.where(pd.notna(df), None)

    # Int64 nullable → object para que executemany reciba None correctamente
    for col in df.select_dtypes(include=["Int64"]).columns:
        df[col] = df[col].astype(object).where(df[col].notna(), None)

    return df, filas_descartadas


# ── Inserción ─────────────────────────────────────────────
def insert_chunk(conn, df: pd.DataFrame):
    cur = conn.cursor()
    columnas_presentes = [c for c in COLUMNAS_SCHEMA if c in df.columns]
    placeholders = ", ".join(["%s"] * len(columnas_presentes))
    cols_str = ", ".join(columnas_presentes)
    sql = f"INSERT INTO mediciones ({cols_str}) VALUES ({placeholders})"
    rows = [tuple(r) for r in df[columnas_presentes].itertuples(index=False, name=None)]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()


# ── ETA helpers ───────────────────────────────────────────
def fmt_eta(segundos: float) -> str:
    if segundos < 0 or math.isinf(segundos):
        return "calculando…"
    td = timedelta(seconds=int(segundos))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    if td.days:
        return f"{td.days}d {h:02d}h {m:02d}m"
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


# ── Main ──────────────────────────────────────────────────
def main():
    checkpoint = load_checkpoint()
    chunk_index_inicio = checkpoint["chunk_index"]
    filas_insertadas   = checkpoint["filas_insertadas"]
    filas_descartadas  = checkpoint.get("filas_descartadas", 0)

    conn = get_connection()
    setup_database(conn)

    # Contar filas para ETA
    print("📊 Contando filas del CSV…")
    with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
        total_filas = sum(1 for _ in f) - 1  # descontar encabezado
    total_chunks = math.ceil(total_filas / CHUNK_SIZE)
    print(f"   Total filas : {total_filas:,}")
    print(f"   Chunks ({CHUNK_SIZE:,}/c): {total_chunks:,}\n")

    reader = pd.read_csv(
        CSV_PATH,
        chunksize=CHUNK_SIZE,
        dtype=str,
        encoding="utf-8",
        errors="replace",
        on_bad_lines="skip",
    )

    tiempos_chunk: list[float] = []   # para media móvil de ETA

    bar_fmt = (
        "{l_bar}{bar}| {n_fmt}/{total_fmt} chunks "
        "[{elapsed}<{remaining}, {rate_fmt}]"
    )

    with tqdm(
        total=total_chunks,
        initial=chunk_index_inicio,
        desc="Insertando",
        unit=" chunk",
        bar_format=bar_fmt,
    ) as pbar:

        for chunk_index, chunk in enumerate(reader):

            if chunk_index < chunk_index_inicio:
                continue  # saltar chunks ya procesados

            t0 = time.perf_counter()

            try:
                df_limpio, n_desc = limpiar_chunk(chunk)

                if len(df_limpio) > 0:
                    insert_chunk(conn, df_limpio)

                filas_insertadas  += len(df_limpio)
                filas_descartadas += n_desc
                save_checkpoint(chunk_index + 1, filas_insertadas, filas_descartadas)

                # ETA con media móvil de los últimos 10 chunks
                t1 = time.perf_counter()
                tiempos_chunk.append(t1 - t0)
                if len(tiempos_chunk) > 10:
                    tiempos_chunk.pop(0)
                avg_t = sum(tiempos_chunk) / len(tiempos_chunk)
                chunks_restantes = total_chunks - (chunk_index + 1)
                eta_seg = avg_t * chunks_restantes

                pbar.update(1)
                pbar.set_postfix({
                    "✅ inserts": f"{filas_insertadas:,}",
                    "⚠️ descart": f"{filas_descartadas:,}",
                    "ETA": fmt_eta(eta_seg),
                    "s/chunk": f"{avg_t:.1f}s",
                })

            except Exception as e:
                print(f"\n❌ Error en chunk {chunk_index}: {e}")
                print("💾 Checkpoint guardado — vuelve a correr el script para reanudar")
                conn.rollback()
                conn.close()
                return

    conn.close()

    # Limpiar checkpoint al terminar exitosamente
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)

    pct_desc = (filas_descartadas / total_filas * 100) if total_filas else 0
    print(f"\n{'='*55}")
    print(f"✅ Carga completa")
    print(f"   Filas insertadas : {filas_insertadas:,}")
    print(f"   Filas descartadas: {filas_descartadas:,}  ({pct_desc:.2f}%)")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()