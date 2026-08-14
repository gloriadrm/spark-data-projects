# spark-lab

Laboratorio de práctica deliberada de Apache Spark. No es un tutorial: cada mini
proyecto tiene un objetivo de habilidad muy concreto, para llegar con soltura al
proyecto grande (Real-Time Air Traffic Analytics con Dataproc + Kafka + Terraform).

## Filosofía

- Un mini proyecto = una habilidad. No se mezclan joins, window functions, regex
  y streaming en el mismo ejercicio.
- Criterio de salida por bloque: no se pasa al siguiente proyecto por haberlo
  "terminado", sino cuando el siguiente dataset se aborda sin mirar la
  documentación todo el rato.
- El repo `spark-lab` en sí es el activo de aprendizaje. Los proyectos grandes
  (vuelos, etc.) son los que se enseñan; estos son los que hacen que se sepa.


----
- Transforma datos → muévelo a transform.py.
- Genera métricas, rankings o tablas analíticas → puede mantenerse como analysis.py.
- Solo coordina llamadas → muévelo a main.py.
- Comprueba calidad → muévelo a quality.py.
- Escribe resultados → muévelo a load.py.

Estructura recomendada:
src/
├── extract.py
├── transform.py
├── analysis.py
├── load.py
├── quality.py
└── main.py

---
## Estructura

```
spark-lab/
├── common/            # SparkSession y utilidades compartidas entre proyectos
├── datasets/           # datasets descargados (no versionar los pesados)
├── mini-projects/
│   ├── 01-sales-etl/          -> API básica: read, filter, select, withColumn, joins, groupBy
│   ├── 02-apache-logs/        -> texto: regexp, split, fechas, cache
│   ├── 03-nyc-taxi/           -> window functions: rank, dense_rank, lag, lead
│   ├── 04-json-processing/    -> JSON anidado: explode, arrays, structs
│   ├── 05-medallion/          -> arquitectura Bronze → Silver → Gold, particionado
│   └── 06-spark-sql/          -> mismo problema con DataFrame API y con Spark SQL
└── notes/              # apuntes sueltos, errores encontrados, trucos
```

Cada mini proyecto sigue la misma plantilla (README, data/, notebook.ipynb, src/,
output/) para coger hábitos que luego se trasladan al proyecto grande.

## Progreso

| # | Proyecto | Concepto núcleo | Estado |
|---|----------|-----------------|--------|
| 01 | Sales ETL | read/filter/select/withColumn/joins/groupBy | ⬜ |
| 02 | Apache Logs | regexp/split/dates/cache | ⬜ |
| 03 | NYC Taxi | window/rank/lag/lead | ⬜ |
| 04 | JSON Processing | explode/arrays/structs | ⬜ |
| 05 | Medallion | bronze/silver/gold/particionado | ⬜ |
| 06 | Spark SQL | DataFrame API vs SQL | ⬜ |

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
