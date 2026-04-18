"""
bronze.py — ETL Bronze Layer: ingesta de CSVs locales a S3 y Glue Data Catalog.

Este script forma parte del pipeline de vuelos. Su responsabilidad es subir
los archivos CSV originales a S3 en formato Parquet sin ninguna transformación,
y registrarlos en el Glue Catalog dentro de la base de datos `flights_bronze`.

Uso:
    python bronze.py --bucket <nombre-del-bucket> --data-dir <ruta-local/data>

Ejemplo:
    python bronze.py --bucket my-flights-bucket --data-dir ./data

Rutas S3 de destino:
    s3://<bucket>/bronze/flights/
    s3://<bucket>/bronze/airlines/
    s3://<bucket>/bronze/airports/
"""

import argparse
import logging
import os
import sys

import awswrangler as wr
import pandas as pd

# ---------------------------------------------------------------------------
# Parámetros
# ---------------------------------------------------------------------------
FLIGHTS_DTYPES = {
    # Identifiers
    "YEAR":               "Int16",
    "MONTH":              "Int8",
    "DAY":                "Int8",
    "DAY_OF_WEEK":        "Int8",
    "AIRLINE":            "str",
    "FLIGHT_NUMBER":      "Int32",
    "FLIGHT_NUMBER":      "str",
    "DISTANCE":           "Int32",
    "SCHEDULED_DEPARTURE": "str",
    "DEPARTURE_TIME":      "str",
    "WHEELS_OFF":          "str",
    "SCHEDULED_TIME":      "Int32",
    "ELAPSED_TIME":        "Int32",
    "AIR_TIME":            "Int32",
    "WHEELS_ON":           "str",
    "SCHEDULED_ARRIVAL":   "str",
    "ARRIVAL_TIME":        "str",
    "DEPARTURE_DELAY":     "Int32",
    "TAXI_OUT":            "Int32",
    "TAXI_IN":             "Int32",
    "ARRIVAL_DELAY":       "Int32",
    "AIR_SYSTEM_DELAY":    "Int32",
    "SECURITY_DELAY":      "Int32",
    "AIRLINE_DELAY":       "Int32",
    "LATE_AIRCRAFT_DELAY": "Int32",
    "WEATHER_DELAY":       "Int32",
    # Booleans
    "DIVERTED":            "Int8",
    "CANCELLED":           "Int8",
}



# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    """
    Configura y retorna el logger del módulo.

    Formato: timestamp — nivel — mensaje.

    Returns:
        logging.Logger: Logger configurado con nivel INFO y salida a stdout.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_data(path: str, table_name: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Lee un archivo CSV desde el sistema de archivos local.

    Valida únicamente que el archivo exista y que sea legible. Las validaciones
    de calidad del dato se delegan a transform(). Si el archivo no existe o
    falla la lectura, registra el error y termina con código de salida 1.

    Args:
        path (str): Ruta absoluta o relativa al archivo CSV.
        table_name (str): Nombre de la tabla.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame con el contenido íntegro del CSV.

    Raises:
        SystemExit: Si el archivo no existe o falla la lectura.
    """
    logger.info(f"[{table_name}] Leyendo CSV desde: {path}")

    if not os.path.exists(path):
        logger.error(f"[{table_name}] Archivo no encontrado: {path}")
        sys.exit(1)

    try:
        dtypes = FLIGHTS_DTYPES if table_name == "flights" else None
        df = pd.read_csv(path, low_memory=False, dtype=dtypes)
    except Exception:
        logger.exception(f"[{table_name}] Error al leer el CSV.")
        sys.exit(1)

    logger.info(f"[{table_name}] CSV leído correctamente. Filas: {len(df):,} | Columnas: {len(df.columns)}")
    return df

# ---------------------------------------------------------------------------
# Creación de base de datos
# ---------------------------------------------------------------------------

def create_bronze_database(db_name: str, logger: logging.Logger) -> None:
    """
    Crea la base de datos Silver en el Glue Data Catalog si no existe.

    Usa `exist_ok=True` para garantizar idempotencia: si la base de datos
    ya existe.

    Args:
        db_name (str): Nombre de la base de datos a crear en Glue.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si la creación de la base de datos falla con código 1.
    """
    try:
        wr.catalog.create_database(name=db_name, exist_ok=True)
        logger.info(f"Base de datos Glue '{db_name}' lista.")
    except Exception:
        logger.exception(f"No se pudo crear la base de datos '{db_name}' en Glue.")
        sys.exit(1)

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

# Columnas que no pueden contener nulos por tabla.

KEY_COLUMNS: dict[str, list[str]] = {
    "flights":  ["year", "month", "day", "airline", "flight_number",
                 "origin_airport", "destination_airport"],
    "airlines": ["iata_code", "airline"],
    "airports": ["iata_code", "airport", "city", "country"],
}


TIME_COLS = [
    "scheduled_departure", "departure_time", "wheels_off",
    "wheels_on", "scheduled_arrival", "arrival_time",
]

def _format_time_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Convierte enteros tipo 1505 a '15:05', manejando NaN y ceros iniciales.
    """
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.zfill(4)          # 515 → '0515'
                .str.replace(r"(\d{2})(\d{2})", r"\1:\2", regex=True)  # '0515' → '05:15'
                .replace({"nan": pd.NA, "<NA>": pd.NA})
            )
    return df
    
def transform_data(df: pd.DataFrame, table_name: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Valida la calidad mínima del dato y normaliza los nombres de columna.

    Proceso:
      1. Normalizar nombres de columna (strip, lowercase, espacios → '_').
      2. Verificar que el DataFrame no está vacío.
      3. Verificar que las columnas clave definidas en KEY_COLUMNS existen.
      4. Verificar que esas columnas clave no contienen nulos.

    Args:
        df (pd.DataFrame): DataFrame crudo leído desde el CSV.
        table_name (str): Nombre de la tabla; determina qué columnas
                          clave se validan según KEY_COLUMNS.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame con columnas normalizadas y datos validados.

    Raises:
        AssertionError: Si el DataFrame está vacío, faltan columnas clave
                        o éstas contienen valores nulos.
    """
    # 1. Normalizar nombres de columna
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    logger.info(f"[{table_name}] Columnas normalizadas: {list(df.columns)}")
    
    # 2. Castear columnas
    if table_name == "flights":
        df = _format_time_cols(df, TIME_COLS)
        # Solo castear a str las columnas object que NO son time ni ya fueron procesadas
        object_cols = [c for c in df.select_dtypes("object").columns if c not in TIME_COLS]
        if object_cols:
            df[object_cols] = df[object_cols].astype(str).replace("nan", pd.NA)
    else:
        mixed_cols = df.select_dtypes(include="object").columns.tolist()
        if mixed_cols:
            df[mixed_cols] = df[mixed_cols].astype(str).replace("nan", pd.NA)
        
    # 3. El DataFrame no debe estar vacío
    assert not df.empty, (
        f"[{table_name}] El DataFrame está vacío — abortando antes de escribir a S3."
    )

    # 4 & 5. Validar columnas clave
    key_cols = KEY_COLUMNS.get(table_name, [])
    if not key_cols:
        logger.warning(f"[{table_name}] No hay columnas clave definidas en KEY_COLUMNS — omitiendo validación de nulos.")
    else:
        missing = [col for col in key_cols if col not in df.columns]
        assert not missing, (
            f"[{table_name}] Columnas clave ausentes en el CSV: {missing}. "
            f"Columnas disponibles: {list(df.columns)}"
        )

        for col in key_cols:
            null_count = df[col].isnull().sum()
            assert null_count == 0, (
                f"[{table_name}] La columna clave '{col}' tiene {null_count:,} valor(es) nulo(s) — "
                f"abortando antes de escribir a S3."
            )

        logger.info(f"[{table_name}] Validación OK — columnas clave sin nulos: {key_cols}")

    return df


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_to_s3(
    df: pd.DataFrame,
    bucket: str,
    db_name: str,
    table_name: str,
    logger: logging.Logger,
    ) -> str:
    """
    Escribe el DataFrame en S3 como Parquet y lo registra en el Glue Data Catalog.

    Garantiza idempotencia usando mode='overwrite': si la tabla ya existe en S3
    y en Glue, se sobreescribe por completo en cada ejecución. Registra el número
    de filas cargadas y la ruta S3 de destino al finalizar.

    Args:
        df (pd.DataFrame): DataFrame validado listo para persistir.
        bucket (str): Nombre del bucket S3 de destino (sin prefijo s3://).
        db_name (str): Nombre de la base de datos en el Glue Catalog.
        table_name (str): Nombre de la tabla en S3 y en Glue.
        logger (logging.Logger): Logger del módulo.

    Returns:
        str: Ruta S3 donde se escribieron los archivos Parquet.

    Raises:
        SystemExit: Si la escritura a S3 o el registro en Glue fallan.
    """
    s3_path = f"s3://{bucket}/bronze/{table_name}/"

    try:
        logger.info(f"[{table_name}] Subiendo {len(df):,} filas a: {s3_path}")
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=db_name,
            table=table_name,
            mode="overwrite",
        )
    except Exception:
        logger.exception(f"[{table_name}] Fallo al escribir en S3/Glue.")
        sys.exit(1)

    logger.info(f"[{table_name}] Carga completada — {len(df):,} filas → {s3_path}")
    return s3_path


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def main(bucket: str, data_dir: str, logger: logging.Logger) -> None:
    """
    Orquesta el pipeline Bronze: extract → transform → load para cada tabla.

    Crea la base de datos `flights_bronze` en el Glue Catalog (con
    exist_ok=True para garantizar idempotencia) y procesa cada dataset en orden.
    Al finalizar imprime un resumen con filas cargadas y rutas S3 por tabla.

    Args:
        bucket (str): Nombre del bucket S3 de destino.
        data_dir (str): Ruta local al directorio raíz que contiene los CSVs.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si la creación de la base de datos o cualquier paso del
                    pipeline falla de forma inesperada.
    """
    db_name = "flights_bronze"

    # Mapa tabla - ruta relativa dentro de data_dir
    datasets = {
        "flights":  os.path.join("flights", "flights.csv"),
        "airlines": os.path.join("flights", "airlines.csv"),
        "airports": os.path.join("flights", "airports.csv"),
    }

    # Paso 1 — Crear base de datos Bronze en Glue
    create_bronze_database(db_name, logger)

    # Paso 2- Lectura desde local, transformación y carga en S3
    summary: list[dict] = []

    for table_name, relative_path in datasets.items():
        full_path = os.path.join(data_dir, relative_path)

        df = extract_data(full_path, table_name, logger)
        df = transform_data(df, table_name, logger)
        s3_path = load_to_s3(df, bucket, db_name, table_name, logger)

        summary.append({"table": table_name, "rows": len(df), "s3_path": s3_path})

    # Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN — Bronze pipeline finalizado")
    logger.info("=" * 60)
    for entry in summary:
        logger.info(f"  {entry['table']:<12} {entry['rows']:>10,} filas  →  {entry['s3_path']}")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL Bronze Layer — Ingesta de CSVs locales a S3 y Glue Catalog.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Nombre del bucket S3 de destino (sin prefijo s3://).",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Ruta local al directorio raíz que contiene la carpeta flights/.",
    )
    args = parser.parse_args()

    _logger = setup_logging()
    _logger.info("Iniciando pipeline Bronze.")
    _logger.info(f"Bucket: {args.bucket} | Data dir: {args.data_dir}")

    main(bucket=args.bucket, data_dir=args.data_dir, logger=_logger)