# 06 · Spark Partitioning and Performance

# Objetivo

Analizar el impacto del particionado, los shuffles y las estrategias de persistencia
sobre la ejecucion de una carga de trabajo Spark.

# Dataset

Datos de NYC Taxi (`datasets/02-nyc-taxi`), reutilizados por su volumen y formato Parquet.

# Preguntas que responder

- ¿Como cambia el tiempo de procesamiento con distinto numero de particiones?
- ¿Que impacto tiene `repartition` frente a `coalesce`?
- ¿Cuando compensa un broadcast join frente a un join convencional?
- ¿Como afecta `partitionBy` en la escritura al problema de los ficheros pequeños?

# Transformaciones previstas

- `repartition` / `coalesce` con distinto numero de particiones.
- Particionado fisico en la escritura con `partitionBy` (año, mes).
- `cache` / `persist` y comparacion de tiempos con y sin persistencia.
- Inspeccion de planes con `explain()` e identificacion de shuffles.
- Comparacion entre joins convencionales y broadcast joins.

# Resultado esperado

```
results/
├── experiment_results.csv
└── conclusions.md
```

Un documento de conclusiones que relacione volumen de datos, distribucion de
particiones, movimiento de informacion (shuffle) y coste de escritura.

## Lecciones aprendidas

_(rellenar al terminar el proyecto)_
