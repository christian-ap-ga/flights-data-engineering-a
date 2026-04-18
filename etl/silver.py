"""
silver.py — ETL Silver Layer: agregaciones de Bronze a Glue Data Catalog.

Lee la tabla `flights_bronze.flights` desde S3, aplica transformaciones y
construye tres tablas agregadas que se escriben en la base de datos
`flights_silver` del Glue Data Catalog:

  - flights_daily      — métricas por día (particionada por mes)
  - flights_monthly    — métricas por mes y aerolínea
  - flights_by_airport — métricas por aeropuerto de origen

Cada tabla pasa validaciones de calidad con `assert` antes de escribirse a S3.
El script es idempotente: elimina las tablas existentes en Glue antes de
escribir y usa mode='overwrite' / 'overwrite_partitions' según corresponda.

Uso:
    python silver.py --bucket <nombre-del-bucket>

Ejemplo:
    python silver.py --bucket my-flights-bucket

Rutas S3 de destino:
    s3://<bucket>/silver/flights_daily/
    s3://<bucket>/silver/flights_monthly/
    s3://<bucket>/silver/flights_by_airport/
"""

import argparse
import logging
import sys

import awswrangler as wr
import pandas as pd
import numpy as np


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

def extract_data(bucket: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Lee la tabla `flights_bronze.flights` desde S3 en formato Parquet.

    Usa awswrangler para leer directamente desde el path S3 registrado en
    el Glue Catalog. Si la lectura falla, registra el error y termina con 
    código de salida 1.

    Args:
        bucket (str): Nombre del bucket S3 donde reside Bronze.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame con el contenido deflights_bronze.flights.

    Raises:
        SystemExit: Si la lectura desde S3 falla.
    """
    s3_path = f"s3://{bucket}/bronze/flights/"
    logger.info(f"[extract] Leyendo flights desde: {s3_path}")

    try:
        df = wr.s3.read_parquet(path=s3_path, dataset=True)
    except Exception:
        logger.exception("[extract] Error al leer flights desde S3.")
        sys.exit(1)

    logger.info(f"[extract] Lectura completada. Filas: {len(df):,} | Columnas: {len(df.columns)}")
    return df

# ---------------------------------------------------------------------------
# Validaciones
# ---------------------------------------------------------------------------

def _cast_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Castea columnas a numérico de forma segura, convirtiendo errores a NaN.

    Args:
        df (pd.DataFrame): DataFrame fuente.
        cols (list[str]): Nombres de columnas a castear.

    Returns:
        pd.DataFrame: DataFrame con las columnas indicadas convertidas a float64.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _validate(df: pd.DataFrame, table_name: str, key_cols: list[str]) -> None:
    """
    Valida calidad mínima del DataFrame agregado antes de escribirlo a S3.

    Comprueba que:
      1. El DataFrame no está vacío.
      2. Las columnas clave existen en el DataFrame.
      3. Las columnas clave no contienen valores nulos.

    Args:
        df (pd.DataFrame): DataFrame agregado a validar.
        table_name (str): Nombre de la tabla.
        key_cols (list[str]): Columnas que no pueden tener nulos.

    Raises:
        AssertionError: Si el DataFrame está vacío, faltan columnas clave o
                        éstas contienen valores nulos.
    """
    assert not df.empty, (
        f"[{table_name}] El DataFrame está vacío — abortando antes de escribir a S3."
    )

    missing = [col for col in key_cols if col not in df.columns]
    assert not missing, (
        f"[{table_name}] Columnas clave ausentes: {missing}. "
        f"Disponibles: {list(df.columns)}"
    )

    for col in key_cols:
        null_count = df[col].isnull().sum()
        assert null_count == 0, (
            f"[{table_name}] La columna clave '{col}' tiene {null_count:,} "
            f"valor(es) nulo(s) — abortando antes de escribir a S3."
        )

# ---------------------------------------------------------------------------
# Creación de base de datos
# ---------------------------------------------------------------------------

def create_silver_database(db_name: str, logger: logging.Logger) -> None:
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

def transform_flights_daily(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Agrega el DataFrame de vuelos a nivel diario.

    Calcula por cada combinación (year, month, day):
      - total_flights       : número total de vuelos del día.
      - total_delayed       : vuelos con departure_delay > 0.
      - total_cancelled     : vuelos con cancelled = 1.
      - avg_departure_delay : retraso promedio de salida, excluyendo cancelados.
      - avg_arrival_delay   : retraso promedio de llegada, excluyendo cancelados.

    Valida el resultado con assert antes de retornarlo.

    Args:
        df (pd.DataFrame): DataFrame crudo de flights_bronze.flights.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame agregado por día listo para escribir a S3.

    Raises:
        AssertionError: Si la agregación produce un DataFrame vacío o con
                        nulos en columnas clave.
    """
    logger.info("[flights_daily] Iniciando agregación diaria.")

    numeric_cols = ["departure_delay", "arrival_delay", "cancelled"]
    df = _cast_numeric(df, numeric_cols)

    # Vuelos no cancelados para promedios de retraso
    not_cancelled = df[df["cancelled"] != 1]

    agg = (
        df.groupby(["year", "month", "day"], as_index=False)
        .agg(
            total_flights=("flight_number", "count"),
            total_delayed=("departure_delay", lambda x: (x > 0).sum()),
            total_cancelled=("cancelled", lambda x: (x == 1).sum()),
        )
    )

    delay_avgs = (
        not_cancelled.groupby(["year", "month", "day"], as_index=False)
        .agg(
            avg_departure_delay=("departure_delay", "mean"),
            avg_arrival_delay=("arrival_delay", "mean"),
        )
    )

    result = agg.merge(delay_avgs, on=["year", "month", "day"], how="left")
    result["avg_departure_delay"] = result["avg_departure_delay"].round(2)
    result["avg_arrival_delay"] = result["avg_arrival_delay"].round(2)

    _validate(result, "flights_daily", key_cols=["year", "month", "day", "total_flights"])
    logger.info(f"[flights_daily] Agregación completada. Filas: {len(result):,}")
    return result


def transform_flights_monthly(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Agrega el DataFrame de vuelos a nivel mensual por aerolínea.

    Calcula por cada combinación (month, airline):
      - total_flights    : número total de vuelos.
      - total_delayed    : vuelos con departure_delay > 0.
      - total_cancelled  : vuelos con cancelled = 1.
      - avg_arrival_delay: retraso promedio de llegada, excluyendo cancelados.
      - on_time_pct      : porcentaje de vuelos con arrival_delay <= 15.

    Valida el resultado con assert antes de retornarlo.

    Args:
        df (pd.DataFrame): DataFrame crudo de flights_bronze.flights.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame agregado por mes y aerolínea listo para S3.

    Raises:
        AssertionError: Si la agregación produce un DataFrame vacío o con
                        nulos en columnas clave.
    """
    logger.info("[flights_monthly] Iniciando agregación mensual por aerolínea.")

    numeric_cols = ["departure_delay", "arrival_delay", "cancelled"]
    df = _cast_numeric(df, numeric_cols)

    not_cancelled = df[df["cancelled"] != 1]

    agg = (
        df.groupby(["month", "airline"], as_index=False)
        .agg(
            total_flights=("flight_number", "count"),
            total_delayed=("departure_delay", lambda x: (x > 0).sum()),
            total_cancelled=("cancelled", lambda x: (x == 1).sum()),
        )
    )

    delay_avgs = (
        not_cancelled.groupby(["month", "airline"], as_index=False)
        .agg(
            avg_arrival_delay=("arrival_delay", "mean"),
            on_time_pct=("arrival_delay", lambda x: round((x <= 15).sum() / len(x) * 100, 2)),
        )
    )

    result = agg.merge(delay_avgs, on=["month", "airline"], how="left")
    result["avg_arrival_delay"] = result["avg_arrival_delay"].round(2)

    _validate(result, "flights_monthly", key_cols=["month", "airline", "total_flights"])
    logger.info(f"[flights_monthly] Agregación completada. Filas: {len(result):,}")
    return result


def transform_flights_by_airport(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Agrega el DataFrame de vuelos a nivel de aeropuerto de origen.

    Calcula por cada origin_airport:
      - total_departures    : total de vuelos que salieron de ese aeropuerto.
      - total_delayed       : vuelos con departure_delay > 0.
      - total_cancelled     : vuelos con cancelled = 1.
      - avg_departure_delay : retraso promedio de salida, excluyendo cancelados.
      - pct_weather_delay   : porcentaje del total de minutos de retraso
                              atribuidos a condiciones climáticas.

    Valida el resultado con assert antes de retornarlo.

    Args:
        df (pd.DataFrame): DataFrame crudo de flights_bronze.flights.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pd.DataFrame: DataFrame agregado por aeropuerto listo para S3.

    Raises:
        AssertionError: Si la agregación produce un DataFrame vacío o con
                        nulos en columnas clave.
    """
    logger.info("[flights_by_airport] Iniciando agregación por aeropuerto de origen.")

    numeric_cols = ["departure_delay", "cancelled", "weather_delay",
                    "air_system_delay", "security_delay", "airline_delay",
                    "late_aircraft_delay"]
    df = _cast_numeric(df, numeric_cols)

    not_cancelled = df[df["cancelled"] != 1]

    agg = (
        df.groupby("origin_airport", as_index=False)
        .agg(
            total_departures=("flight_number", "count"),
            total_delayed=("departure_delay", lambda x: (x > 0).sum()),
            total_cancelled=("cancelled", lambda x: (x == 1).sum()),
            total_weather_delay_min=("weather_delay", "sum"),
            total_delay_min=("departure_delay", lambda x: x[x > 0].sum()),
        )
    )

    delay_avgs = (
        not_cancelled.groupby("origin_airport", as_index=False)
        .agg(avg_departure_delay=("departure_delay", "mean"))
    )

    result = agg.merge(delay_avgs, on="origin_airport", how="left")

    # pct_weather_delay: minutos de retraso por clima / total minutos de retraso
    result["pct_weather_delay"] = (
        result["total_weather_delay_min"]
        / result["total_delay_min"].replace(0, np.nan)
        * 100
    ).round(2)

    result = result.drop(columns=["total_weather_delay_min", "total_delay_min"])
    result["avg_departure_delay"] = result["avg_departure_delay"].round(2)

    _validate(result, "flights_by_airport", key_cols=["origin_airport", "total_departures"])
    logger.info(f"[flights_by_airport] Agregación completada. Filas: {len(result):,}")
    return result


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_to_s3(
    df: pd.DataFrame,
    bucket: str,
    db_name: str,
    table_name: str,
    logger: logging.Logger,
    partition_cols: list[str] | None = None,
    mode: str = "overwrite",
    ) -> str:
    """
    Escribe el DataFrame en S3 como Parquet+Snappy y lo registra en Glue.

    Args:
        df (pd.DataFrame): DataFrame agregado y validado listo para persistir.
        bucket (str): Nombre del bucket S3 de destino (sin prefijo s3://).
        db_name (str): Nombre de la base de datos en el Glue Data Catalog.
        table_name (str): Nombre de la tabla en S3 y en Glue.
        logger (logging.Logger): Logger del módulo.
        partition_cols (list[str] | None): Columnas de partición. None = sin partición.
        mode (str): Modo de escritura de awswrangler ('overwrite' o
                    'overwrite_partitions'). Por defecto 'overwrite'.

    Returns:
        str: Ruta S3 donde se escribieron los archivos Parquet.

    Raises:
        SystemExit: Si la escritura a S3 o el registro en Glue fallan.
    """
    s3_path = f"s3://{bucket}/silver/{table_name}/"

    try:
        # Idempotencia: eliminar entrada preexistente en Glue para evitar
        # conflictos de ruta si el script se corre más de una vez.
        wr.catalog.delete_table_if_exists(database=db_name, table=table_name)
        logger.info(f"[{table_name}] Tabla preexistente eliminada de Glue (idempotencia).")

        logger.info(f"[{table_name}] Escribiendo {len(df):,} filas a: {s3_path}")
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=db_name,
            table=table_name,
            mode=mode,
            compression="snappy",
            partition_cols=partition_cols,
        )
    except Exception:
        logger.exception(f"[{table_name}] Fallo al escribir en S3/Glue.")
        sys.exit(1)

    logger.info(f"[{table_name}] Carga completada — {len(df):,} filas → {s3_path}")
    return s3_path


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def main(bucket: str, logger: logging.Logger) -> None:
    """
    Orquesta el pipeline Silver para cada tabla.

    Flujo:
      1. Crea la base de datos `flights_silver` en Glue (idempotente).
      2. Lee flights_bronze.flights una sola vez desde S3.
      3. Genera las tres tablas agregadas en paralelo lógico (secuencial).
      4. Escribe cada tabla a S3 con sus parámetros específicos de partición.
      5. Imprime un resumen final con filas y rutas por tabla.

    Args:
        bucket (str): Nombre del bucket S3 de destino.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si la creación de la base de datos o cualquier paso del
                    pipeline falla de forma inesperada.
    """
    db_name = "flights_silver"

    # Paso 1 — Crear base de datos Silver en Glue
    create_silver_database(db_name, logger)

    # Paso 2 - EXtracción de datos (tabla flights)
    df_raw = extract_data(bucket, logger)

    # Definición de tablas: (función de transform, partition_cols, mode)
    tables = [
        {
            "name":           "flights_daily",
            "transform_fn":   transform_flights_daily,
            "partition_cols": ["month"],
            "mode":           "overwrite_partitions",
        },
        {
            "name":           "flights_monthly",
            "transform_fn":   transform_flights_monthly,
            "partition_cols": None,
            "mode":           "overwrite",
        },
        {
            "name":           "flights_by_airport",
            "transform_fn":   transform_flights_by_airport,
            "partition_cols": None,
            "mode":           "overwrite",
        },
    ]

    summary: list[dict] = []

    # Paso 3 - Transformación de tablas y carga a S3
    for table in tables:
        table_name = table["name"]

        try:
            df_agg = table["transform_fn"](df_raw.copy(), logger)
        except AssertionError:
            logger.exception(f"[{table_name}] Validación fallida — abortando.")
            sys.exit(1)

        s3_path = load_to_s3(
            df=df_agg,
            bucket=bucket,
            db_name=db_name,
            table_name=table_name,
            logger=logger,
            partition_cols=table["partition_cols"],
            mode=table["mode"],
        )

        summary.append({"table": table_name, "rows": len(df_agg), "s3_path": s3_path})

    # Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN — Silver pipeline finalizado")
    logger.info("=" * 60)
    for entry in summary:
        logger.info(f"  {entry['table']:<22} {entry['rows']:>8,} filas  →  {entry['s3_path']}")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL Silver Layer — Agregaciones de Bronze a Glue Catalog.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Nombre del bucket S3 de destino (sin prefijo s3://).",
    )
    args = parser.parse_args()

    _logger = setup_logging()
    _logger.info("Iniciando pipeline Silver.")
    _logger.info(f"Bucket: {args.bucket}")

    main(bucket=args.bucket, logger=_logger)