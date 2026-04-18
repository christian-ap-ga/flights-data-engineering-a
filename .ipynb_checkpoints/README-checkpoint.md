# Flights Data Engineering — Pipeline End-to-End
Autor: Christian Aparicio García

Pipeline de datos completo sobre el dataset de vuelos domésticos de EE.UU. (2015, ~5.8M vuelos).
Implementa una arquitectura **Medallion Bronze → Silver → Gold** sobre AWS S3 + Athena,
un modelo relacional en **PostgreSQL (RDS)**, y análisis estadístico con regresión lineal
y pronóstico de series de tiempo.
 
---
 
## Estructura del repositorio
 
```
flights-data-engineering-a/
│
├── data/                          ← CSVs fuente
│   ├── flights/flights.csv
│   ├── flights/airlines.csv
│   └── flights/airports.csv
│
├── etl/                           ← Scripts de producción ETL
│   ├── bronze.py                  ← Ingesta: CSV → S3 + Glue
│   ├── silver.py                  ← Agregaciones: Bronze → Silver Parquet
│   ├── gold.py                    ← CTAS en Athena: Bronze → Gold desnormalizado
│   └── rds_load.py                ← Bulk insert a PostgreSQL vía SQLAlchemy 2.0
│
├── notebooks/
│   ├── flights_analytics.ipynb    ← P1–P5, W1–W3 + Regresión OLS + Series de tiempo StatsForecast
│
├── infra/
│   └── rds-flights.yaml           ← CloudFormation template (RDS + Read Replica)
│
├── docs/
│   ├── erd-flights.drawio         ← Diagrama ERD fuente (draw.io)
│   ├── erd-flights.png            ← Diagrama ERD exportado
│   └── screenshots/               ← Evidencia de ejecución
│       ├── 01_bronze/
│       ├── 02_silver/
│       ├── 03_gold/
│       ├── 04_rds/
│       └── 05_dbeaver/
│
├── .gitignore
└── README.md
```
 
---
 
## Arquitectura
 
```
CSVs locales
     │
     ▼  etl/bronze.py
┌─────────────────────────────────┐
│  Bronze — S3 + Glue Catalog     │  flights_bronze.{flights, airlines, airports}
│  s3://<bucket>/flights/bronze/  │  Parquet sin transformaciones — fuente de verdad
└─────────────────────────────────┘
     │
     ▼  etl/silver.py
┌─────────────────────────────────┐
│  Silver — S3 + Glue Catalog     │  flights_daily (part. por mes)
│  s3://<bucket>/flights/silver/  │  flights_monthly
│  Parquet + Snappy               │  flights_by_airport
└─────────────────────────────────┘
     │
     ▼  etl/gold.py
┌─────────────────────────────────┐
│  Gold — Athena CTAS             │  flights_gold.vuelos_analitica
│  s3://<bucket>/flights/gold/    │  Join vuelos + aerolíneas + aeropuertos
└─────────────────────────────────┘
     │
     ▼  etl/rds_load.py
┌─────────────────────────────────┐
│  PostgreSQL RDS                 │  airlines, airports, flights (500K rows)
│  Primaria: escrituras           │  Credenciales vía Secrets Manager
│  Read Replica: consultas        │
└─────────────────────────────────┘
```
 
---
 
## Dataset
 
| Archivo | Filas | Descripción |
|---------|-------|-------------|
| `flights.csv` | ~5.8M | Un registro por vuelo: fechas, aerolínea, origen, destino, demoras, cancelaciones |
| `airlines.csv` | 14 | Catálogo de aerolíneas: código IATA y nombre completo |
| `airports.csv` | 322 | Catálogo de aeropuertos: código IATA, nombre, ciudad, estado, lat/lon |

 
## Diagrama ERD
 
![ERD Flights](docs/ERD-Flights.drawio.png)
 
### Relaciones
 
| Relación | Tipo | Descripción |
|----------|------|-------------|
| `flights.airline` → `airlines.iata_code` | N:1 | Cada vuelo opera una aerolínea |
| `flights.origin_airport` → `airports.iata_code` | N:1 | Cada vuelo sale de un aeropuerto |
| `flights.destination_airport` → `airports.iata_code` | N:1 | Cada vuelo llega a un aeropuerto |
 
> `flights` tiene **dos FK** a `airports` (origen y destino) — relación auto-referencial al mismo catálogo.
 
---
 
## Cómo correr el pipeline
 
### 1. Bronze — Ingesta a S3
 
```bash
python etl/bronze.py \
    --bucket <bucket> \
    --data-dir data/
```
 
Rutas S3 de destino:
- `s3://<bucket>/bronze/flights/`
- `s3://<bucket>/bronze/airlines/`
- `s3://<bucket>/bronze/airports/`
### 2. Silver — Agregaciones
 
```bash
python etl/silver.py \
    --bucket <bucket>
```
 
Tablas generadas en `flights_silver`:
- `flights_daily` — particionada por `month`
- `flights_monthly`
- `flights_by_airport`
### 3. Gold — CTAS en Athena
 
```bash
python etl/gold.py \
    --bucket <bucket>
```
 
Tabla resultante: `flights_gold.vuelos_analitica`
 
### 4. Carga a PostgreSQL RDS
 
```bash
python etl/rds_load.py \
    --secret-id   <credentials> \
    --rds-endpoint <host>.us-east-1.rds.amazonaws.com \
    --data-dir    data/ \
    --region      us-east-1
```
 
> Las credenciales se obtienen desde **AWS Secrets Manager**.
> El endpoint es el **primario**. Las consultas analíticas usan la Read Replica.
 
---
 
## Infraestructura RDS (CloudFormation)
 
El template `infra/rds-flights.yaml` provisiona:
- Instancia RDS PostgreSQL `db.t3.micro`
- Read Replica en otra AZ
- Subnet group y security group
- Credenciales en AWS Secrets Manager
### Despliegue
 
```bash
aws cloudformation create-stack \
    --stack-name itam-flights-rds \
    --template-body file://infra/rds-flights.yaml \
    --parameters \
        ParameterKey=DBName,ParameterValue=flights \
        ParameterKey=DBUsername,ParameterValue=itam \
        ParameterKey=DBPassword,ParameterValue=<tu-password> \
        ParameterKey=CreateReadReplica,ParameterValue=true \
    --capabilities CAPABILITY_IAM
```

---
 
## Notebooks
 
### `notebooks/flights_analytics.ipynb`
 
Conecta a la **Read Replica** vía SQLAlchemy + `pd.read_sql` y ejecuta las 8 preguntas analíticas:
 
| ID | Pregunta | Visualización |
|----|----------|---------------|
| P1 | Top 10 rutas más frecuentes | `plotnine` — barras horizontales |
| P2 | Top 5 aerolíneas por cancelación | `plotnine` — barras horizontales |
| P3 | Distribución de razones de cancelación | `great_tables` — tabla con degradado |
| P4 | Retraso promedio de salida por mes | `plotnine` — línea de tiempo |
| P5 | Top 10 aeropuertos por weather delay | `plotnine` — barras horizontales |
| W1 | Mayor retraso de llegada por aerolínea | `great_tables` — ranking con degradado |
| W2 | Variación mes a mes — LAG() | `plotnine` — línea de tiempo |
| W3 | Primeros 5 vuelos del día en LAX | `great_tables` — horario estilizado |
 
Incluye además la sección **8.1 — Regresión Lineal** con `statsmodels.OLS`:
- Coeficientes con IC al 95%
- Predichos vs. reales
- Residuos vs. predichos
- Q-Q plot de residuos
- R², RMSE e interpretación
> **W2** se ejecuta sobre `flights_silver.flights_monthly` en Athena con `wr.athena.read_sql_query()`.
> PostgreSQL solo contiene 500K filas (enero–feb 2015); LAG() con 2 meses no es representativo.

 
Sección **8.2 — Pronóstico de Series de Tiempo** con StatsForecast (Nixtla):
 
- **Datos:** `flights_silver.flights_monthly` vía Athena
- **Modelos:** `AutoETS`, `AutoARIMA`, `AutoTheta` con `season_length=12`
- **Split:** train = ene–sep 2015 | test = oct–dic 2015
- **Horizonte:** 9 pasos (3 test + 6 meses futuros: ene–jun 2016)
- **IC:** bandas de confianza al 90%
Visualizaciones:
1. Evaluación sobre el test set (pronóstico vs. real oct–dic 2015)
2. Pronóstico 6 meses hacia adelante (ene–jun 2016)
3. Comparación de MAE por modelo
---
 
## Buenas prácticas implementadas
 
Todos los scripts ETL siguen las mismas convenciones de producción:
 
| Práctica | Implementación |
|----------|----------------|
| `logging` | `logging.basicConfig` con timestamp, nivel y mensaje. Sin `print()`. |
| `argparse` | `--bucket`, `--data-dir`, `--region`, etc. Sin hardcodeo. |
| `try/except` + exit codes | `logger.exception()` + `sys.exit(1)` en pasos críticos. |
| `assert` antes de escribir | DataFrame no vacío, columnas clave sin nulos, tipos correctos. |
| Idempotencia | `mode="overwrite"`, `delete_table_if_exists()`, `exist_ok=True`. |
| `main()` | Funciones `extract()`, `transform()`, `load()` con responsabilidad única. |
 
---
 
## Evidencia — Screenshots
 
### Bronze
 
| Archivo | Descripción |
|---------|-------------|
| `docs/screenshots/01_bronze/aws-glue-flights-bronze.png` | Consola Glue — base de datos `flights_bronze` con las 3 tablas |
| `docs/screenshots/01_bronze/s3_bronze_paths.png` | Rutas S3 en el bucket |
 
### Silver
 
| Archivo | Descripción |
|---------|-------------|
| `docs/screenshots/02_silver/aws-glue-flights-silver.png` | Consola Glue — base de datos `flights_silver` con las 3 tablas |
| `docs/screenshots/02_silver/aws-glue-flights-partition.png` | Particiones de `flights_daily` por mes |
 
### Gold
 
| Archivo | Descripción |
|---------|-------------|
| `docs/screenshots/03_gold/aws-athena-flights-gold.png` | `SELECT * FROM flights_gold.vuelos_analitica LIMIT 5` en Athena |
 
### RDS / PostgreSQL
 
| Archivo | Descripción |
|---------|-------------|
| `docs/screenshots/04_rds/aws-cf-rds-stack.png` | Stack `itam-flights-rds` en estado `CREATE_COMPLETE` con Outputs |
| `docs/screenshots/04_rds/dbeaver-schema.png` | Confirmación de las tres tablas con la cantidad de datos|
 
### DBeaver — Queries
 
| Archivo | Descripción |
|---------|-------------|
| `docs/screenshots/05_dbeaver/db-p1.png` | P1 — Top 10 rutas más frecuentes |
| `docs/screenshots/05_dbeaver/db-p2.png` | P2 — Top 5 aerolíneas por cancelación |
| `docs/screenshots/05_dbeaver/db-p3.png` | P3 — Cancelaciones por razón |
| `docs/screenshots/05_dbeaver/db-p4.png` | P4 — Retraso promedio por mes |
| `docs/screenshots/05_dbeaver/db-p5.png` | P5 — Top 10 aeropuertos por weather delay |
| `docs/screenshots/05_dbeaver/db-p6.png` | W1 — RANK() mayor retraso por aerolínea |
| `docs/screenshots/05_dbeaver/db-p7.png` | W3 — ROW_NUMBER() primeros 5 vuelos LAX |
 
---
 
## Tecnologías
 
| Capa | Tecnología |
|------|------------|
| Almacenamiento | AWS S3 |
| Catálogo de metadatos | AWS Glue Data Catalog |
| Consultas analíticas | AWS Athena |
| Base de datos relacional | AWS RDS PostgreSQL + Read Replica |
| Infraestructura como código | AWS CloudFormation |
| Secretos | AWS Secrets Manager |
| ETL | Python, `awswrangler`, `pandas` |
| ORM | SQLAlchemy 2.0 + `psycopg2` |
| Visualización | `plotnine`, `great_tables` |
| Regresión | `statsmodels` |
| Pronóstico | `statsforecast` (Nixtla) — AutoETS, AutoARIMA, AutoTheta |
| Cliente SQL | DBeaver Community Edition |
