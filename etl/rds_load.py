"""
rds_load.py — ETL RDS Layer: carga de CSVs a PostgreSQL vía SQLAlchemy 2.0.

Este script forma parte del pipeline de vuelos. Su responsabilidad es insertar
los datos de los tres CSVs fuente en una base de datos PostgreSQL en Amazon RDS,
respetando el orden de dependencias FK definido en el ERD:

  1. airlines  — sin dependencias
  2. airports  — sin dependencias
  3. flights   — depende de airlines (airline → iata_code)
                 y de airports (origin_airport, destination_airport → iata_code)

Las credenciales se obtienen exclusivamente desde AWS Secrets Manager.
El endpoint RDS se recibe como argumento externo.

Uso:
    python etl/rds_load.py \\
        --secret-id   itam/rds/flights/credentials \\
        --rds-endpoint <host>.us-east-1.rds.amazonaws.com \\
        --data-dir    data/ \\
        [--region     us-east-1]

Para flights se cargan únicamente los primeros 500,000 registros.
"""

import argparse
import json
import logging
import sys
from typing import Optional

import boto3
import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    create_engine,
    insert,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    """
    Configura y retorna el logger del módulo.

    Formato: timestamp — nivel — mensaje. Se usa logging estándar en lugar de
    print() para garantizar trazabilidad cuando el pipeline corre sin supervisión.

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
# Modelos SQLAlchemy 2.0
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """Base declarativa compartida por todos los modelos del pipeline."""
    pass


class Airline(Base):
    """
    Tabla de aerolíneas.

    Corresponde al CSV airlines.csv. Su PK (iata_code) es referenciada
    por flights.airline como FK.
    """
    __tablename__ = "airlines"

    iata_code: Mapped[str]           = mapped_column(String(10),  primary_key=True)
    airline:   Mapped[str]           = mapped_column(String(100), nullable=False)


class Airport(Base):
    """
    Tabla de aeropuertos.

    Corresponde al CSV airports.csv. Su PK (iata_code) es referenciada
    por flights.origin_airport y flights.destination_airport como FK.
    """
    __tablename__ = "airports"

    iata_code:  Mapped[str]            = mapped_column(String(10),  primary_key=True)
    airport:    Mapped[str]            = mapped_column(String(200), nullable=False)
    city:       Mapped[str]            = mapped_column(String(100), nullable=False)
    state:      Mapped[Optional[str]]  = mapped_column(String(50))
    country:    Mapped[str]            = mapped_column(String(100), nullable=False)
    latitude:   Mapped[Optional[float]] = mapped_column(Float)
    longitude:  Mapped[Optional[float]] = mapped_column(Float)


class Flight(Base):
    """
    Tabla de vuelos.

    Corresponde al CSV flights.csv. Referencia airports e airlines mediante
    FKs. No tiene PK natural definida en el ERD.

    """
    __tablename__ = "flights"

    flight_id:           Mapped[int]            = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    year:                Mapped[int]            = mapped_column(SmallInteger, nullable=False)
    month:               Mapped[int]            = mapped_column(SmallInteger, nullable=False)
    day:                 Mapped[int]            = mapped_column(SmallInteger, nullable=False)
    day_of_week:         Mapped[int]            = mapped_column(SmallInteger, nullable=False)
    airline:             Mapped[str]            = mapped_column(String(10),  ForeignKey("airlines.iata_code"), nullable=False)
    flight_number:       Mapped[int]            = mapped_column(Integer,     nullable=False)
    tail_number:         Mapped[Optional[str]]  = mapped_column(String(10))
    origin_airport:      Mapped[str]            = mapped_column(String(10),  ForeignKey("airports.iata_code"), nullable=False)
    destination_airport: Mapped[str]            = mapped_column(String(10),  ForeignKey("airports.iata_code"), nullable=False)
    scheduled_departure: Mapped[Optional[str]]  = mapped_column(String(5))   # formato HH:MM
    departure_time:      Mapped[Optional[str]]  = mapped_column(String(5))
    departure_delay:     Mapped[Optional[int]]  = mapped_column(Integer)
    taxi_out:            Mapped[Optional[int]]  = mapped_column(Integer)
    wheels_off:          Mapped[Optional[str]]  = mapped_column(String(5))
    scheduled_time:      Mapped[Optional[int]]  = mapped_column(Integer)
    elapsed_time:        Mapped[Optional[int]]  = mapped_column(Integer)
    air_time:            Mapped[Optional[int]]  = mapped_column(Integer)
    distance:            Mapped[Optional[int]]  = mapped_column(Integer)
    wheels_on:           Mapped[Optional[str]]  = mapped_column(String(5))
    taxi_in:             Mapped[Optional[int]]  = mapped_column(Integer)
    scheduled_arrival:   Mapped[Optional[str]]  = mapped_column(String(5))
    arrival_time:        Mapped[Optional[str]]  = mapped_column(String(5))
    arrival_delay:       Mapped[Optional[int]]  = mapped_column(Integer)
    diverted:            Mapped[Optional[bool]] = mapped_column(Boolean)
    cancelled:           Mapped[Optional[bool]] = mapped_column(Boolean)
    cancellation_reason: Mapped[Optional[str]]  = mapped_column(String(1))
    air_system_delay:    Mapped[Optional[int]]  = mapped_column(Integer)
    security_delay:      Mapped[Optional[int]]  = mapped_column(Integer)
    airline_delay:       Mapped[Optional[int]]  = mapped_column(Integer)
    late_aircraft_delay: Mapped[Optional[int]]  = mapped_column(Integer)
    weather_delay:       Mapped[Optional[int]]  = mapped_column(Integer)


# ---------------------------------------------------------------------------
# Extract — Secrets Manager
# ---------------------------------------------------------------------------

def get_secret(secret_id: str, region: str, logger: logging.Logger) -> dict:
    """
    Obtiene y parsea las credenciales RDS desde AWS Secrets Manager.

    Args:
        secret_id (str): ARN o nombre del secreto en Secrets Manager.
        region (str): Región AWS donde está el secreto (ej. 'us-east-1').
        logger (logging.Logger): Logger del módulo.

    Returns:
        dict: Diccionario con claves username, password, dbname, port.

    Raises:
        SystemExit: Si no se puede obtener o parsear el secreto.
    """
    logger.info(f"Obteniendo credenciales desde Secrets Manager: {secret_id}")
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_id)
        creds = json.loads(response["SecretString"])
    except Exception:
        logger.exception("Error al obtener el secreto desde Secrets Manager.")
        sys.exit(1)

    required_keys = {"username", "password", "dbname", "port"}
    missing = required_keys - creds.keys()
    if missing:
        logger.error(f"El secreto no contiene las claves requeridas: {missing}")
        sys.exit(1)

    logger.info("Credenciales obtenidas correctamente.")
    return creds


# ---------------------------------------------------------------------------
# Conexión a RDS
# ---------------------------------------------------------------------------

def build_engine(creds: dict, rds_endpoint: str, logger: logging.Logger):
    """
    Construye y verifica el engine de SQLAlchemy apuntando al endpoint primario.

    Args:
        creds (dict): Credenciales obtenidas desde Secrets Manager.
        rds_endpoint (str): Host del endpoint primario de RDS.
        logger (logging.Logger): Logger del módulo.

    Returns:
        sqlalchemy.Engine: Engine listo para usar.

    Raises:
        SystemExit: Si la conexión falla.
    """
    url = (
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{rds_endpoint}:{creds['port']}/{creds['dbname']}"
    )
    logger.info(f"Conectando a RDS: {rds_endpoint}:{creds['port']}/{creds['dbname']}")

    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexión a RDS verificada exitosamente.")
    except Exception:
        logger.exception("Error al conectar con RDS.")
        sys.exit(1)

    return engine


# ---------------------------------------------------------------------------
# Transform — preparación de registros
# ---------------------------------------------------------------------------

def _format_time_col(series: pd.Series) -> pd.Series:
    """
    Convierte una columna de tiempos enteros (ej. 1505) al formato 'HH:MM'.

    Args:
        series (pd.Series): Columna con valores tipo 1505, 830, 0, NaN.

    Returns:
        pd.Series: Columna con valores tipo '15:05', '08:30', '00:00', None.
    """
    def _convert(val):
        if pd.isnull(val):
            return None
        s = str(int(float(val))).zfill(4)
        return f"{s[:2]}:{s[2:]}"

    return series.apply(_convert)


TIME_COLS = [
    "scheduled_departure", "departure_time", "wheels_off",
    "wheels_on", "scheduled_arrival", "arrival_time",
]

FLIGHTS_NROWS = 500_000


def prepare_airlines(data_dir: str, logger: logging.Logger) -> list[dict]:
    """
    Lee y prepara los registros de airlines para bulk insert.

    Normaliza nombres de columna a minúsculas y convierte NaN a None.

    Args:
        data_dir (str): Ruta al directorio raíz que contiene los CSVs.
        logger (logging.Logger): Logger del módulo.

    Returns:
        list[dict]: Lista de dicts listos para session.execute(insert(Airline), ...).

    Raises:
        SystemExit: Si el archivo no existe o la lectura falla.
        AssertionError: Si el DataFrame está vacío o iata_code tiene nulos.
    """
    path = f"{data_dir}/flights/airlines.csv"
    logger.info(f"[airlines] Leyendo CSV desde: {path}")

    try:
        df = pd.read_csv(path)
    except Exception:
        logger.exception("[airlines] Error al leer el CSV.")
        sys.exit(1)

    df.columns = [c.strip().lower() for c in df.columns]

    assert not df.empty, "[airlines] El DataFrame está vacío."
    assert df["iata_code"].isnull().sum() == 0, "[airlines] iata_code tiene nulos."

    records = [{k: None if pd.isnull(v) else v for k, v in row.items()}
               for row in df.to_dict(orient="records")]
    logger.info(f"[airlines] {len(records):,} registros preparados.")
    return records


def prepare_airports(data_dir: str, logger: logging.Logger) -> list[dict]:
    """
    Lee y prepara los registros de airports para bulk insert.

    Normaliza nombres de columna a minúsculas y convierte NaN a None.

    Args:
        data_dir (str): Ruta al directorio raíz que contiene los CSVs.
        logger (logging.Logger): Logger del módulo.

    Returns:
        list[dict]: Lista de dicts listos para session.execute(insert(Airport), ...).

    Raises:
        SystemExit: Si el archivo no existe o la lectura falla.
        AssertionError: Si el DataFrame está vacío o iata_code tiene nulos.
    """
    path = f"{data_dir}/flights/airports.csv"
    logger.info(f"[airports] Leyendo CSV desde: {path}")

    try:
        df = pd.read_csv(path)
    except Exception:
        logger.exception("[airports] Error al leer el CSV.")
        sys.exit(1)

    df.columns = [c.strip().lower() for c in df.columns]

    assert not df.empty, "[airports] El DataFrame está vacío."
    assert df["iata_code"].isnull().sum() == 0, "[airports] iata_code tiene nulos."

    records = [{k: None if pd.isnull(v) else v for k, v in row.items()}
               for row in df.to_dict(orient="records")]
    logger.info(f"[airports] {len(records):,} registros preparados.")
    return records


def prepare_flights(data_dir: str, logger: logging.Logger) -> list[dict]:
    """
    Lee y prepara los primeros 500,000 registros de flights para bulk insert.

    Aplica las siguientes transformaciones antes de insertar:
      - Normalización de nombres de columna a minúsculas.
      - Casteo de columnas numéricas con nullable integers.
      - Conversión de columnas time de formato entero (1505) a 'HH:MM'.
      - Conversión de NaN/NaT a None para compatibilidad con PostgreSQL.
      - Eliminación de flight_id.

    Args:
        data_dir (str): Ruta al directorio raíz que contiene los CSVs.
        logger (logging.Logger): Logger del módulo.

    Returns:
        list[dict]: Lista de dicts listos para session.execute(insert(Flight), ...).

    Raises:
        SystemExit: Si el archivo no existe o la lectura falla.
        AssertionError: Si el DataFrame está vacío o columnas clave tienen nulos.
    """
    path = f"{data_dir}/flights/flights.csv"
    logger.info(f"[flights] Leyendo CSV desde: {path} (nrows={FLIGHTS_NROWS:,})")

    try:
        df = pd.read_csv(path, nrows=FLIGHTS_NROWS, low_memory=False)
    except Exception:
        logger.exception("[flights] Error al leer el CSV.")
        sys.exit(1)

    df.columns = [c.strip().lower() for c in df.columns]

    # Casteo de columnas numéricas a nullable int
    int_cols = [
        "year", "month", "day", "day_of_week", "flight_number",
        "departure_delay", "taxi_out", "scheduled_time", "elapsed_time",
        "air_time", "distance", "taxi_in", "arrival_delay",
        "air_system_delay", "security_delay", "airline_delay",
        "late_aircraft_delay", "weather_delay",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

    # Formato HH:MM para columnas de tiempo
    for col in TIME_COLS:
        if col in df.columns:
            df[col] = _format_time_col(df[col])

    # Validaciones
    assert not df.empty, "[flights] El DataFrame está vacío."
    key_cols = ["year", "month", "day", "airline", "flight_number",
                "origin_airport", "destination_airport"]
    for col in key_cols:
        null_count = df[col].isnull().sum()
        assert null_count == 0, (
            f"[flights] Columna clave '{col}' tiene {null_count:,} nulos."
        )

    # Convertir Int32 nullable a Python int/None para SQLAlchemy
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.isnull(v) if not isinstance(v, str) else False:
                clean[k] = None
            elif hasattr(v, "item"):
                clean[k] = v.item()
            else:
                clean[k] = v
        records.append(clean)

    logger.info(f"[flights] {len(records):,} registros preparados.")
    return records


# ---------------------------------------------------------------------------
# Load — bulk insert
# ---------------------------------------------------------------------------

def bulk_insert(
    session: Session,
    model,
    records: list[dict],
    table_name: str,
    logger: logging.Logger,
    ) -> None:
    """
    Ejecuta un bulk insert de registros en la tabla indicada.

    Usa el patrón de SQLAlchemy 2.0: session.execute(insert(Model), records),
    que emite un único INSERT con múltiples VALUES.

    Args:
        session (Session): Sesión SQLAlchemy activa.
        model: Clase ORM (Airline, Airport, Flight).
        records (list[dict]): Lista de dicts con los registros a insertar.
        table_name (str): Nombre lógico de la tabla (usado en logs).
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si el insert falla.
    """
    logger.info(f"[{table_name}] Insertando {len(records):,} registros en RDS.")
    try:
        session.execute(insert(model), records)
    except Exception:
        logger.exception(f"[{table_name}] Error en bulk insert.")
        sys.exit(1)
    logger.info(f"[{table_name}] Insert completado.")


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def main(
    secret_id: str,
    rds_endpoint: str,
    data_dir: str,
    region: str,
    logger: logging.Logger,
    ) -> None:
    """
    Orquesta el pipeline RDS: obtención de credenciales, DDL y bulk insert.

    Flujo:
      1. Obtiene credenciales desde Secrets Manager.
      2. Construye y verifica el engine contra el endpoint primario.
      3. Ejecuta drop_all + create_all para garantizar idempotencia del schema.
      4. Prepara los registros de cada tabla (extract + transform).
      5. Inserta en orden FK: airlines, airports, flights.
      6. Hace commit.

    Args:
        secret_id (str): ARN o nombre del secreto en Secrets Manager.
        rds_endpoint (str): Host del endpoint primario de RDS.
        data_dir (str): Ruta local al directorio raíz con los CSVs.
        region (str): Región AWS del secreto y del cluster RDS.
        logger (logging.Logger): Logger del módulo.

    Raises:
        SystemExit: Si cualquier paso crítico falla.
    """
    # 1. Credenciales
    creds = get_secret(secret_id, region, logger)

    # 2. Conexión
    engine = build_engine(creds, rds_endpoint, logger)

    # 3. DDL — idempotencia: drop en orden inverso de FK, luego create
    logger.info("Recreando schema (drop_all + create_all).")
    try:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        logger.info("Schema creado correctamente.")
    except Exception:
        logger.exception("Error al crear el schema en RDS.")
        sys.exit(1)

    # 4. Preparar registros (fuera de la sesión)
    try:
        airlines_records = prepare_airlines(data_dir, logger)
        airports_records = prepare_airports(data_dir, logger)
        flights_records  = prepare_flights(data_dir, logger)
    except AssertionError:
        logger.exception("Validación de datos fallida — abortando antes de insertar.")
        sys.exit(1)

    # 5 & 6. Insert atómico respetando orden FK
    summary = []
    with Session(engine) as session:
        bulk_insert(session, Airline, airlines_records, "airlines", logger)
        bulk_insert(session, Airport, airports_records, "airports", logger)
        bulk_insert(session, Flight,  flights_records,  "flights",  logger)

        logger.info("Ejecutando commit.")
        try:
            session.commit()
        except Exception:
            logger.exception("Error en commit — se hará rollback automático.")
            sys.exit(1)

        summary = [
            {"table": "airlines", "rows": len(airlines_records)},
            {"table": "airports", "rows": len(airports_records)},
            {"table": "flights",  "rows": len(flights_records)},
        ]

    # Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN — RDS pipeline finalizado")
    logger.info("=" * 60)
    for entry in summary:
        logger.info(f"  {entry['table']:<12} {entry['rows']:>10,} filas insertadas")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL RDS Layer — Bulk insert de CSVs a PostgreSQL en RDS.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--secret-id",
        required=True,
        help="ARN o nombre del secreto en AWS Secrets Manager con las credenciales RDS.",
    )
    parser.add_argument(
        "--rds-endpoint",
        required=True,
        help="Host del endpoint primario de RDS (sin puerto).",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Ruta local al directorio raíz con la carpeta flights/.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="Región AWS donde está el secreto y el cluster RDS.",
    )
    args = parser.parse_args()

    _logger = setup_logging()
    _logger.info("Iniciando pipeline RDS.")
    _logger.info(f"Endpoint: {args.rds_endpoint} | Secret: {args.secret_id} | Data dir: {args.data_dir}")

    main(
        secret_id=args.secret_id,
        rds_endpoint=args.rds_endpoint,
        data_dir=args.data_dir,
        region=args.region,
        logger=_logger,
    )