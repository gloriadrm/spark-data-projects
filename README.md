# spark-lab

Laboratorio de práctica deliberada de Apache Spark (PySpark). No es un tutorial: cada mini
proyecto tiene un objetivo de habilidad muy concreto, pensado para llegar con soltura al
proyecto grande (Real-Time Air Traffic Analytics con Dataproc + Kafka + Terraform).

## Estructura

```
spark-lab/
├── common/                            # SparkSession y utilidades de perfilado compartidas
├── datasets/                          # datasets descargados (no versionados)
├── projects/
│   ├── 01-sales-etl/                  # API básica: read, filter, select, withColumn, joins, groupBy
│   ├── 02-nyc-taxi-windows/           # window functions: rank, dense_rank, row_number, lag/lead
│   ├── 03-yelp-nested-json/           # JSON anidado: explode, arrays, structs
│   ├── 04-medallion-architecture/     # arquitectura Bronze → Silver → Gold, particionado
│   ├── 05-spark-sql/                  # el mismo problema con DataFrame API y con Spark SQL
│   ├── 06-partitioning-performance/   # shuffles, particionado y estrategias de persistencia
│   └── 07-churn-telco-prediction/     # clasificación distribuida con Spark MLlib
└── notes/                             # apuntes sueltos, errores encontrados, trucos
```

Cada mini proyecto sigue la misma plantilla (README, notebook de exploración, `src/`,
`output/`) para coger hábitos que luego se trasladan al proyecto grande. La lógica de `src/`
se consolida en un sprint aparte una vez completados todos los notebooks; hasta entonces es
solo scaffolding y no se versiona.

## Progreso

| # | Proyecto | Concepto núcleo | Estado |
|---|----------|-----------------|--------|
| 01 | Sales ETL | read / filter / select / withColumn / joins / groupBy | Completado |
| 02 | NYC Taxi Windows | window functions: rank / dense_rank / lag / lead | Completado |
| 03 | Yelp Nested JSON | explode / arrays / structs | Completado |
| 04 | Medallion Architecture | bronze / silver / gold, particionado | En curso |
| 05 | Spark SQL | DataFrame API vs SQL | Pendiente |
| 06 | Partitioning & Performance | shuffles, particionado, persistencia | Pendiente |
| 07 | Churn Telco Prediction | Spark MLlib | Pendiente |

## Convenciones de `src/`

Una vez un mini proyecto se da por explorado, su lógica se reparte así:

- Transforma datos → `transform.py`.
- Genera métricas, rankings o tablas analíticas → `analysis.py`.
- Comprueba calidad de datos → `quality.py`.
- Escribe resultados → `load.py`.
- Coordina las llamadas a los módulos anteriores → `main.py`.

## Setup

Requiere Python 3.13 y Java 17 (OpenJDK). Detalles completos en `projects/SETUP.md`.

```bash
uv sync
```
