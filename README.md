# spark-lab

Laboratorio de práctica deliberada de Apache Spark (PySpark). No es un tutorial: cada mini
proyecto tiene un objetivo de habilidad muy concreto, pensado para llegar con soltura al
proyecto grande (Real-Time Air Traffic Analytics con Dataproc + Kafka + Terraform).

## Estructura

```
spark-lab/
├── common/                            # SparkSession y utilidades compartidas (perfilado, calidad, escritura)
├── datasets/                          # datasets descargados (no versionados)
├── projects/
│   ├── 01-sales-etl/                  # API básica: read, filter, select, withColumn, joins, groupBy
│   ├── 02-nyc-taxi-windows/           # window functions: rank, dense_rank, row_number, lag/lead
│   ├── 03-yelp-nested-json/           # JSON anidado: explode, arrays, structs
│   ├── 04-medallion-architecture/     # arquitectura Bronze → Silver → Gold, particionado
│   ├── 05-spark-sql/                  # el mismo problema con DataFrame API y con Spark SQL
│   ├── 06-partitioning-performance/   # shuffles, particionado y estrategias de persistencia
│   └── 07-churn-telco-prediction/     # clasificación con Spark MLlib
└── notes/                             # apuntes sueltos, errores encontrados, trucos
```

Cada mini proyecto sigue la misma plantilla (README, notebook y `output/`) para coger hábitos
que luego se trasladan al proyecto grande.

## Progreso

| # | Proyecto | Concepto núcleo | Estado |
|---|----------|-----------------|--------|
| 01 | Sales ETL | read / filter / select / withColumn / joins / groupBy | Completado |
| 02 | NYC Taxi Windows | window functions: rank / dense_rank / lag / lead | Completado |
| 03 | Yelp Nested JSON | explode / arrays / structs | Completado |
| 04 | Medallion Architecture | bronze / silver / gold, particionado | Completado |
| 05 | Spark SQL | DataFrame API vs SQL, Catalyst y planes de ejecución | Completado |
| 06 | Partitioning & Performance | shuffles, particionado, persistencia, broadcast join, partition pruning | Completado |
| 07 | Churn Telco Prediction | Spark MLlib: pipelines, evaluación, umbral de decisión, interpretabilidad | Completado · mejoras pendientes |

### Resultados destacados

- **01 · Sales ETL (Olist):** validación de claves primarias e integridad referencial entre 9 tablas. Todas las relaciones se mantienen salvo 13 productos con 2 categorías sin traducción, y la relación pedidos → reseñas no es 1:1 (547 pedidos con varias reseñas).
- **02 · NYC Taxi Windows:** los nulos de 5 columnas (955.371 registros) no eran pérdidas aleatorias, sino el esquema propio de los viajes Flex Fare. Para calcular variaciones hora a hora con `lag()` se construyó una rejilla completa zona × hora (`crossJoin`), porque `groupBy` no genera filas para las horas sin viajes.
- **03 · Yelp Nested JSON:** `attributes` y `hours` son structs de esquema fijo, no maps, así que se aplanan con notación de punto sin `explode()`. Se detectaron 73 negocios con el texto literal "None" en lugar de un nulo real.
- **04 · Medallion Architecture:** pipeline Bronze → Silver → Gold sobre 8,6 millones de registros. Declarar el schema redujo la lectura de 6,3 s a 0,05 s, y dos anomalías geoespaciales aparentemente independientes (149 registros cada una) resultaron ser las mismas filas.
- **05 · Spark SQL:** DataFrame API y Spark SQL generan el mismo Physical Plan en 3 de 4 casos; la única diferencia (un `Project` adicional) no altera los shuffles. La sintaxis no cambia la ejecución gracias a Catalyst.
- **06 · Partitioning & Performance:** un Parquet con 4 row groups genera 8 particiones de lectura, de las que solo 4 tienen datos. `coalesce` es ~7,7× más rápido que `repartition`, y el broadcast join ~4,4× más rápido que el SortMergeJoin.
- **07 · Churn Telco Prediction:** regresión logística con AUC-ROC de 0,861; con umbral 0,4 detecta el 72 % de los clientes que abandonan. Con datos que caben en memoria, scikit-learn obtiene las mismas métricas más de un orden de magnitud más rápido.

## Utilidades compartidas (`common/`)

| Módulo | Contenido |
|---|---|
| `spark_session.py` | `create_spark_session()`: sesión local reutilizable (`shuffle.partitions = 4`, zona horaria UTC) |
| `profiling.py` | Perfilado de DataFrames: `duplicate_count`, `missing_values_profile` (nulos, NaN, blancos y marcadores de texto), `cardinality_profile`, `frequency_profile`, `numeric_summary`, `quantile_summary` |
| `relational.py` | Comprobaciones de integridad: `primary_key_check`, `foreign_key_check` |
| `utils.py` | Escritura en Parquet y visualización de esquema y muestra |
| `config.py` | Rutas base del repositorio |

## Próximos pasos

- **Modularización:** trasladar la lógica validada en los notebooks a scripts ejecutables y reutilizables.
- **Sistema de trabajo para ML con Spark MLlib:** plantilla y utilidades reutilizables (`common/`) equivalentes al flujo de trabajo con scikit-learn, aplicadas después al proyecto 07 (validación cruzada con `CrossValidator`, selección de features para reducir la multicolinealidad).
- **Proyecto grande:** Real-Time Air Traffic Analytics con Dataproc + Kafka + Terraform.

## Setup

Requiere Python 3.13, Java 17 (OpenJDK) y PySpark 4. Detalles completos en `projects/SETUP.md`.

```bash
uv sync
```

Spark 4 activa por defecto el **modo ANSI**: un `cast` que no puede convertir un valor lanza un error en lugar de devolver `null`. Para tolerar valores no válidos de forma explícita se usa `try_cast`.
