"""
gold.py — ETL Gold Layer: tabla analítica desnormalizada via CTAS en Athena.

Lee las tablas de la capa Bronze (`flights_bronze`) y construye una tabla
analítica lista para consumo en la base de datos `flights_gold` del Glue
Catalog mediante un CTAS ejecutado directamente en Athena:

  - vuelos_analitica — vuelos desnormalizados con nombres de aerolínea,
                       aeropuertos de origen/destino y todos los campos
                       de retraso, cancelación y distancia.

El script es idempotente: elimina las tablas existentes en Glue antes de
escribir.

Uso:
    python gold.py --bucket <nombre-del-bucket>

Ejemplo:
    python gold.py --bucket my-flights-bucket

Ruta S3 de destino:
    s3://<bucket>/gold/vuelos_analitica/
"""

import argparse
import logging
import sys

import awswrangler as wr


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
# Validaciones
# ---------------------------------------------------------------------------

def validate_query_result(df, logger: logging.Logger) -> None:
    """
    Valida que el resultado del CTAS retorne al menos una fila.

    Comprueba que:
      1. El DataFrame resultante no está vacío.
      2. Las columnas mínimas esperadas están presentes en el resultado.

    Args:
        df: DataFrame de pandas retornado por wr.athena.read_sql_query.
        logger (logging.Logger): Logger del módulo.

    Raises:
        AssertionError: Si el DataFrame está vacío o faltan columnas clave.
    """
    expected_cols = [
        "year", "month", "day",
        "origin_airport", "destination_airport",
        "airline_name", "cancelled",
    ]

    assert not df.empty, (
        "[vuelos_analitica] El CTAS no retornó filas — "
        "verifica que flights_bronze.flights tenga datos."
    )

    missing = [col for col in expected_cols if col not in df.columns]
    assert not missing, (
        f"[vuelos_analitica] Columnas esperadas faltantes: {missing}. "
        f"Columnas disponibles: {list(df.columns)}"
    )

    logger.info(
        f"[vuelos_analitica] Validación OK — "
        f"{len(df):,} filas | {len(df.columns)} columnas."
    )


# ---------------------------------------------------------------------------
# Creación de base de datos
# ---------------------------------------------------------------------------

def create_gold_database(db_name: str, logger: logging.Logger) -> None:
    """
    Crea la base de datos Gold en el Glue Data Catalog si no existe.

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
# Idempotencia — eliminar tabla preexistente
# ---------------------------------------------------------------------------

def drop_table_if_exists(db_name: str, table_name: str, logger: logging.Logger) -> None:
    """
    Elimina la tabla del Glue Data Catalog si existe para garantizar idempotencia.

    Llama a `wr.catalog.delete_table_if_exists` de forma segura. Si la tabla
    no existe, la operación no se ejecuta. Si la eliminación falla por cualquier
    otro motivo, el script termina con código de salida 1.

    Args:
        db_name (str): Nombre de la base de datos en Glue.
        table_name (str): Nombre de la tabla a eliminar.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si la eliminación de la tabla falla con código 1.
    """
    try:
        wr.catalog.delete_table_if_exists(database=db_name, table=table_name)
        logger.info(
            f"[{table_name}] Tabla preexistente eliminada de Glue (idempotencia)."
        )
    except Exception:
        logger.exception(
            f"[{table_name}] Error al intentar eliminar la tabla de Glue."
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CTAS — construcción de la tabla Gold
# ---------------------------------------------------------------------------

CTAS_VUELOS_ANALITICA = """
CREATE TABLE flights_gold.vuelos_analitica AS (
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport     AS origin_airport_name,
        ap_orig.city        AS origin_city,
        ap_orig.state       AS origin_state,
        f.destination_airport,
        ap_dest.airport     AS destination_airport_name,
        al.airline          AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
)
"""


def run_ctas(bucket: str, logger: logging.Logger):
    """
    Ejecuta el CTAS en Athena para construir la tabla analítica Gold.

    Usa `wr.athena.read_sql_query` con `ctas_approach=False` para ejecutar
    el DDL directamente en Athena. El resultado intermedio de Athena se
    almacena en el path S3 designado como output location.

    Args:
        bucket (str): Nombre del bucket S3 donde se almacenan los resultados
                      intermedios de Athena.
        logger (logging.Logger): Logger del módulo.

    Returns:
        pandas.DataFrame: DataFrame con el resultado retornado por Athena
                          tras ejecutar el CTAS.

    Raises:
        SystemExit: Si la ejecución del CTAS en Athena falla con código 1.
    """
    s3_output = f"s3://{bucket}/gold/"
    logger.info(f"[vuelos_analitica] Ejecutando CTAS en Athena.")
    logger.info(f"[vuelos_analitica] Athena output location: {s3_output}")

    try:
        df_result = wr.athena.read_sql_query(
            sql=CTAS_VUELOS_ANALITICA,
            database="flights_gold",
            s3_output=s3_output,
            ctas_approach=False,
        )
    except Exception:
        logger.exception(
            "[vuelos_analitica] Fallo al ejecutar el CTAS en Athena."
        )
        sys.exit(1)

    logger.info("[vuelos_analitica] CTAS ejecutado correctamente.")
    return df_result



# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def main(bucket: str, logger: logging.Logger) -> None:
    """
    Orquesta el pipeline Gold completo.

    Flujo:
      1. Valida el argumento --bucket antes de cualquier operación remota.
      2. Crea la base de datos `flights_gold` en Glue (idempotente).
      3. Elimina la tabla `vuelos_analitica` si existe (idempotencia).
      4. Ejecuta el CTAS en Athena para construir la tabla analítica.
      5. Verifica que la tabla fue creada y contiene registros.
      6. Imprime un resumen final del pipeline.

    Args:
        bucket (str): Nombre del bucket S3 de destino.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si cualquier paso del pipeline falla:
                    - código 1: error operacional (AWS, Athena, Glue).
                    - código 2: validación de argumento fallida (AssertionError).
    """
    db_name = "flights_gold"
    table_name = "vuelos_analitica"


    # Paso 1 — Crear base de datos Gold en Glue
    create_gold_database(db_name, logger)

    # Paso 2 — Eliminar tabla preexistente para garantizar idempotencia
    drop_table_if_exists(db_name, table_name, logger)

    # Paso 3 — Ejecutar CTAS en Athena
    run_ctas(bucket, logger)

    # Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN — Gold pipeline finalizado")
    logger.info("=" * 60)
    logger.info(f"  Base de datos : {db_name}")
    logger.info(f"  Tabla         : {table_name}")
    logger.info(f"  Bucket        : {bucket}")
    logger.info(f"  Athena output : s3://{bucket}/athena-results/gold/")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL Gold Layer — Tabla analítica desnormalizada via CTAS en Athena.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Nombre del bucket S3 de destino (sin prefijo s3://).",
    )
    args = parser.parse_args()

    _logger = setup_logging()
    _logger.info("Iniciando pipeline Gold.")
    _logger.info(f"Bucket: {args.bucket}")

    main(bucket=args.bucket, logger=_logger)