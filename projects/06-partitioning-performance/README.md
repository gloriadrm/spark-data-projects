# 06 · Spark Partitioning & Performance

## 🎯 Objetivo

Analizar cómo la **distribución de los datos entre particiones**, los **shuffles** y distintas estrategias de persistencia, joins y escritura afectan al rendimiento de una carga de trabajo Spark.

El objetivo del proyecto no es únicamente utilizar `repartition`, `coalesce`, `cache`, `broadcast` o `partitionBy`, sino entender **qué ocurre físicamente con los datos**, identificar estas operaciones en los planes de ejecución y razonar cuándo puede ser conveniente utilizar cada estrategia.

---

## 📦 Dataset

Se utilizaron datos de **NYC Taxi** almacenados en formato Parquet.

Características principales del dataset utilizado:

- **4.090.836 registros**
- **66,47 MB** en formato Parquet
- **4 row groups** internos (3 × 1.048.576 filas + 945.108)
- **8 particiones iniciales de Spark**, de las que **solo 4 contienen datos**
- datos de **mayo de 2026** (14 registros con fechas erróneas o fuera del mes, descartados antes de la escritura particionada)

El dataset permitió experimentar con:

- distribución de datos entre particiones;
- agregaciones y shuffles;
- joins entre datasets de distinto tamaño;
- persistencia y reutilización de resultados;
- particionado físico y partition pruning.

---

## 🧠 Conceptos principales

### Particiones de Spark

Una partición representa una unidad de datos que Spark puede procesar mediante una task.

El número y distribución de las particiones afecta al:

- paralelismo;
- uso de recursos;
- movimiento de datos;
- tamaño de las tareas;
- overhead de planificación;
- rendimiento de la ejecución.

Un mayor número de particiones no implica necesariamente mayor rendimiento. Existe un equilibrio entre disponer de suficiente paralelismo y evitar un número excesivo de tareas pequeñas.

---

### Particiones de lectura y row groups

Al leer un fichero, Spark lo divide en *splits* por tamaño:

```text
maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))

bytesPerCore  = (66,47 MB + 4 MB) / 8 cores ≈ 8,8 MB  →  8 splits  →  8 particiones
```

Pero un fichero Parquet está organizado en **row groups**, y un row group no se puede dividir: se lee entero en una única partición. Este fichero tiene 4 row groups de ~16 MB, así que cada uno ocupa dos splits pero solo uno de ellos recibe los datos:

```text
Fichero (66 MB):   [ RG1 ~16 MB ][ RG2 ~16 MB ][ RG3 ~16 MB ][ RG4 ~16 MB ]
Particiones:        [P0] [P1]     [P2] [P3]     [P4] [P5]     [P6] [P7]
                     ✔    ✗        ✔    ✗        ✔    ✗        ✔    ✗
```

Resultado: **8 particiones, 4 con datos**. El paralelismo efectivo de la lectura es 4, no 8. `getNumPartitions()` no describe cómo se reparte realmente el trabajo; hay que inspeccionar el contenido de cada partición (`spark_partition_id()`).

---

### `repartition` vs `coalesce`

Ambas operaciones modifican la distribución de las particiones durante el procesamiento, pero utilizan estrategias diferentes.

- `repartition()` redistribuye los datos mediante un **shuffle**, permitiendo aumentar o disminuir el número de particiones y obtener una distribución más equilibrada.
- `coalesce()` se utiliza principalmente para reducir el número de particiones intentando evitar una redistribución completa de los datos.

El criterio general observado es:

**Reducir particiones sin necesidad de redistribuir → `coalesce`**

**Aumentar particiones o necesitar una redistribución equilibrada → `repartition`**

El trade-off principal es:

**coste del shuffle ↔ distribución de los datos ↔ paralelismo**

---

### Shuffles

Un shuffle implica redistribuir datos entre particiones y puede aparecer en operaciones como:

- `groupBy`
- `join`
- `repartition`
- `distinct`
- `orderBy`
- determinadas agregaciones

Los planes de ejecución se inspeccionaron mediante `explain()`.

La aparición de operaciones **Exchange** en el Physical Plan permitió identificar puntos en los que Spark necesitaba redistribuir datos.

Conviene distinguir dos tipos de particiones:

- las **particiones de entrada**, que se modifican con `repartition` o `coalesce`;
- las **particiones tras un shuffle** (por ejemplo, después de un `groupBy` o un join), controladas por `spark.sql.shuffle.partitions` y ajustadas dinámicamente por **AQE** (Adaptive Query Execution).

En la sesión utilizada, `spark.sql.shuffle.partitions = 4` (el valor por defecto es 200) y AQE estaba activado.

---

### Persistencia: `cache` / `persist`

La persistencia permite almacenar resultados intermedios para evitar su recomputación cuando un mismo DataFrame se reutiliza.

Sin embargo, persistir también tiene un coste:

- materialización inicial;
- consumo de memoria;
- posible utilización de disco;
- gestión de los bloques almacenados.

En el experimento realizado, la cache **empeoró el rendimiento**: la materialización fue costosa y, además, la reutilización resultó más lenta que recalcular desde Parquet. Parte del DataFrame no cabía en memoria y Spark lo almacenó en disco.

---

### Broadcast Join

Se comparó un join convencional con un **BroadcastHashJoin** utilizando una tabla de referencia pequeña.

El join convencional utilizó un **SortMergeJoin**, introduciendo operaciones `Exchange hashpartitioning` para redistribuir los datos.

Mediante broadcast, Spark distribuyó la relación pequeña y evitó el shuffle de la tabla principal.

En el entorno utilizado, `spark.sql.autoBroadcastJoinThreshold` estaba configurado en **10 MB**.

El criterio relevante para seleccionar automáticamente esta estrategia es principalmente el **tamaño estimado de la relación**, no un número determinado de filas.

---

### Particionado físico con `partitionBy`

A diferencia de `repartition` y `coalesce`, que modifican las particiones utilizadas durante el procesamiento, `partitionBy` determina cómo se organizan físicamente los datos al escribirlos.

En el experimento se utilizó una estructura:

```text
year=2026/
└── month=5/
    ├── day=1/
    ├── day=2/
    ├── ...
    └── day=31/
```

Esta organización permitió observar **partition pruning** al realizar consultas filtradas por un día concreto.

La granularidad diaria se utilizó deliberadamente con finalidad experimental. Al contener el dataset únicamente un mes de datos, particionar solo por año y mes ofrecía poca capacidad de pruning.

El objetivo fue observar el trade-off entre:

**mayor granularidad → mayor capacidad de pruning**

y

**mayor granularidad → mayor fragmentación → riesgo de small files**

---

## 🧪 Experimentos

### 1. Baseline

Se caracterizó el dataset antes de aplicar modificaciones:

- número de registros;
- número de particiones y su contenido;
- tamaño físico;
- configuración relevante de Spark;
- tiempo de una carga de trabajo de referencia (`groupBy` por zona de recogida con tres agregaciones).

Hallazgo principal: de las **8 particiones de lectura, solo 4 contienen datos**, una por cada row group del Parquet (ver *Particiones de lectura y row groups*).

La carga de trabajo de referencia obtuvo una mediana de **0,224 s**, que se utilizó como punto de comparación para los experimentos posteriores.

---

### 2. Número de particiones

Se ejecutó la misma carga de trabajo utilizando diferentes números de particiones mediante `repartition`.

Se comprobó que aumentar el número de particiones no produjo automáticamente una mejora de rendimiento.

| Configuración | Particiones | Mediana |
|---|---:|---:|
| Baseline | 8 (4 con datos) | **0,224 s** |
| `repartition(1)` | 1 | 1,109 s |
| `repartition(4)` | 4 | 1,510 s |
| `repartition(8)` | 8 | 1,408 s |
| `repartition(50)` | 50 | 1,776 s |
| `repartition(200)` | 200 | 2,830 s |

En este workload, el coste del shuffle y el overhead asociado a un número creciente de tareas terminaron superando los posibles beneficios del paralelismo adicional. Incluso `repartition(8)`, con 8 particiones equilibradas, fue más lento que el baseline con 4 particiones útiles.

Este experimento modifica las particiones **antes** de la agregación; el número de particiones **después** del shuffle del `groupBy` lo determina `spark.sql.shuffle.partitions`.

---

### 3. `repartition` vs `coalesce`

Se compararon ambas estrategias al reducir el número de particiones.

El Physical Plan permitió observar:

- `Exchange RoundRobinPartitioning` con `repartition`;
- reducción de particiones sin un `Exchange` equivalente con `coalesce`.

| Estrategia | Shuffle | Mediana | Distribución resultante |
|---|:---:|---:|---|
| `repartition(4)` | Sí | 1,226 s | 4 × 1.022.709 |
| `coalesce(4)` | No | 0,160 s | 3 × 1.048.576 + 945.108 |

`coalesce(4)` fue unas **7,7 veces más rápido**. La diferencia se relaciona directamente con el movimiento de datos provocado por el shuffle.

`coalesce(4)` junta particiones consecutivas (0+1, 2+3…). Como las particiones impares estaban vacías, cada partición resultante coincide exactamente con un row group: el reparto sale equilibrado, pero por la estructura del fichero de entrada, no porque `coalesce` rebalancee.

---

### 4. `cache` / `persist`

Se ejecutaron tres acciones sobre un mismo DataFrame transformado, sin cache y con `cache()`. Las ejecuciones sin cache y la reutilización con cache se midieron con la metodología estándar (warm-up + 3 ejecuciones → mediana); la materialización, que solo ocurre una vez, se midió por separado.

| Escenario | Tiempo |
|---|---:|
| Sin cache (mediana) | 0,306 s |
| Materialización de la cache | 3,976 s |
| Reutilización con cache (mediana) | 0,517 s |

La cache **empeoró el rendimiento**, incluso en la reutilización:

- Un bloque del DataFrame no cabía en memoria (`MemoryStore: Not enough space to cache rdd_520_0`) y se almacenó en disco, permitido por el Storage Level `MEMORY_AND_DISK` que usa `cache()`. El aviso se repetía en cada reutilización, al intentar Spark volver a subir el bloque a memoria.
- Sin cache, la lectura de Parquet ya era muy barata: al ser columnar, cada acción solo lee las columnas que necesita, mientras que la cache almacena todas las columnas del DataFrame.

Para una transformación barata de recalcular (un filtro y una columna derivada sobre Parquet), la cache no compensa.

---

### 5. Join convencional vs Broadcast Join

Se compararon:

- `SortMergeJoin`;
- `BroadcastHashJoin`.

| Estrategia | Mediana |
|---|---:|
| `SortMergeJoin` | 1,304 s |
| `BroadcastHashJoin` | 0,298 s |

El broadcast join fue unas **4,4 veces más rápido**.

El Physical Plan mostró que el join convencional requería redistribuir ambos lados mediante `Exchange hashpartitioning(payment_type, 4)`, mientras que el broadcast permitía distribuir únicamente la tabla pequeña (`BroadcastExchange`) y evitar el shuffle de la tabla principal.

En el entorno utilizado, el umbral automático de broadcast estaba configurado en **10 MB**.

---

### 6. Particionado físico en escritura

El dataset se escribió:

- sin particionado físico;
- utilizando `partitionBy("year", "month", "day")`.

La granularidad diaria se utilizó deliberadamente para poder estudiar el comportamiento del partition pruning dentro de un dataset que contiene un único mes.

Antes de escribir se descartaron los **14 registros** con fecha de recogida fuera de mayo de 2026 (fechas de 2008, 2009, 30 de abril y 1 de junio). Sin este filtro, cada uno habría generado su propio directorio de partición con ficheros de apenas unos KB.

#### Ficheros generados

| Estrategia | Nº ficheros | Tamaño total | Tamaño medio |
|---|---:|---:|---:|
| Sin `partitionBy` | 4 | 80,36 MB | 20,09 MB |
| Con `partitionBy` | 65 | 81,89 MB | 1,26 MB |

- **Sin `partitionBy`:** 4 ficheros, uno por cada partición de lectura con datos.
- **Con `partitionBy`:** cada tarea de escritura genera un fichero por cada día que contiene. Tres de los row groups cubren tramos consecutivos del mes, pero el cuarto contiene registros de los 31 días, así que se generan 65 ficheros para 31 días.

La mayor granularidad incrementó considerablemente el número de ficheros y redujo su tamaño medio, permitiendo observar el riesgo asociado al **small files problem**.

#### Lectura filtrada

Para analizar el partition pruning se realizó una consulta filtrando por:

**year = 2026, month = 5, day = 15**

| Lectura | Mediana |
|---|---:|
| Sin `partitionBy` | 0,104 s |
| Con `partitionBy` | 0,047 s |

Para esta consulta concreta, la lectura sobre el dataset particionado fue aproximadamente **2,2 veces más rápida**.

El Physical Plan confirmó la diferencia:

- sin particionado físico, el filtro apareció en **DataFilters** y **PushedFilters**: el lector de Parquet puede usar estadísticas min/max para saltarse row groups, pero sigue abriendo todos los ficheros y filtrando fila a fila (nodo `Filter`);
- con `partitionBy`, el filtro apareció únicamente en **PartitionFilters** y desapareció el nodo `Filter`: el filtrado se resuelve eligiendo qué directorios leer.

Esto demuestra que Spark pudo utilizar la estructura física del dataset para descartar particiones completas antes de procesar sus datos.

El factor de mejora observado es específico de este experimento. Lo generalizable es el mecanismo: cuando los filtros coinciden con las columnas de particionado, **partition pruning puede reducir la cantidad de datos que Spark necesita leer**.

La mayor granularidad presenta, por tanto, un trade-off:

**Mayor granularidad**

→ filtros potencialmente más selectivos  
→ mayor capacidad de partition pruning  
→ menos datos innecesarios leídos  

pero también:

**Mayor granularidad**

→ más particiones físicas  
→ potencialmente más ficheros  
→ menor tamaño medio por fichero  
→ mayor riesgo de small files  
→ mayor overhead de metadatos, apertura de ficheros y planificación  
→ potencial degradación del rendimiento  

El particionado diario utilizado en este experimento tiene una finalidad principalmente experimental y no representa necesariamente el diseño que se utilizaría para este dataset en un escenario real.

---

## ❓ Preguntas respondidas

Los experimentos permiten razonar sobre:

- ¿Cómo decide Spark el número de particiones al leer un fichero Parquet, y por qué pueden quedar vacías?
- ¿Cómo afecta el número de particiones al paralelismo y al tiempo de ejecución?
- ¿Cuándo compensa utilizar `repartition` frente a `coalesce`?
- ¿Qué operaciones provocan shuffles y cómo pueden identificarse en el Physical Plan?
- ¿Cuándo compensa persistir un DataFrame?
- ¿Cuándo puede un broadcast join evitar movimiento innecesario de datos?
- ¿Cómo afecta `partitionBy` a la organización física de los datos?
- ¿Cómo permite el particionado físico realizar partition pruning?
- ¿Qué relación existe entre granularidad, número de ficheros y small files?

---

## 📊 Resultados

Los resultados de los benchmarks se almacenan de forma estructurada para permitir su análisis sin necesidad de volver a ejecutar todos los experimentos.

```text
results/
└── experiment_results.csv
```

`experiment_results.csv` contiene las métricas de cada benchmark: experimento, estrategia, tiempos de las ejecuciones medidas, mediana y número de particiones.

Las conclusiones detalladas de cada experimento, junto con los planes de ejecución y las salidas, están en `notebook.ipynb`.

> Los tiempos corresponden a una ejecución en local (8 cores) y variarán entre ejecuciones y entornos. Lo relevante son las tendencias y los mecanismos observados, no los valores absolutos.

---

## ✅ Checklist de conceptos

- [x] Particiones y paralelismo
- [x] `df.rdd.getNumPartitions()`
- [x] `spark_partition_id()` y particiones vacías
- [x] Row groups de Parquet y cálculo de splits (`maxPartitionBytes`, `openCostInBytes`)
- [x] `spark.sql.shuffle.partitions` y AQE
- [x] `repartition()`
- [x] `coalesce()`
- [x] Shuffle
- [x] `Exchange` en el Physical Plan
- [x] Distribución de datos entre particiones
- [x] `cache()`
- [x] `persist()`
- [x] Storage Level
- [x] Join convencional
- [x] Sort Merge Join
- [x] Broadcast Hash Join
- [x] `spark.sql.autoBroadcastJoinThreshold`
- [x] `partitionBy()`
- [x] Partition pruning
- [x] `DataFilters` / `PushedFilters` vs `PartitionFilters`
- [x] Small files problem
- [x] Lazy evaluation
- [x] Medición de tiempos mediante múltiples ejecuciones y mediana

---

## 💡 Lecciones aprendidas

- **Más particiones no significa automáticamente más rendimiento.** El paralelismo adicional debe compensar el overhead de planificación, coordinación y ejecución de las tasks.

- **El número de particiones no describe cómo se reparte el trabajo.** Un fichero Parquet con 4 row groups generó 8 particiones de lectura, pero solo 4 con datos. Hay que inspeccionar el contenido de las particiones, no solo contarlas.

- **El movimiento de datos es uno de los principales costes en Spark.** Los shuffles pueden identificarse en el Physical Plan mediante operaciones como `Exchange`.

- **`repartition` y `coalesce` no son equivalentes.** `repartition` permite redistribuir los datos mediante shuffle, mientras que `coalesce` puede reducir particiones evitando una redistribución global.

- **Persistir un DataFrame solo compensa cuando es costoso de recalcular y se reutiliza lo suficiente.** En este experimento la cache fue más lenta incluso al reutilizarla: recalcular desde Parquet, leyendo solo las columnas necesarias, era más barato que leer una cache que no cabía en memoria.

- **Cache no significa necesariamente RAM.** El Storage Level utilizado puede permitir que Spark almacene bloques en disco cuando la memoria disponible no es suficiente.

- **Broadcast puede evitar redistribuir una tabla grande cuando la otra relación es suficientemente pequeña.** La decisión depende principalmente de su tamaño estimado y de los recursos disponibles.

- **El particionado durante la ejecución y el particionado físico son conceptos diferentes.** `repartition` y `coalesce` afectan al procesamiento; `partitionBy` afecta a la organización de los datos almacenados.

- **Partition pruning puede reducir los datos que Spark necesita leer**, pero su efectividad depende de que los patrones de consulta utilicen las columnas de particionado y de la selectividad de los filtros.

- **Una granularidad excesiva puede generar small files.** Más granularidad puede mejorar el pruning, pero también incrementar la fragmentación y el overhead asociado a la gestión de ficheros.

- **Las mejoras relativas obtenidas en un benchmark no son generalizables.** Los tiempos dependen del volumen de datos, el workload, la selectividad, los recursos y la configuración del entorno.

- **No existe una configuración universalmente óptima.** La optimización debe basarse en medir el workload, inspeccionar el Physical Plan, identificar el coste dominante, aplicar cambios y volver a medir.

---

## 🔁 Metodología de optimización

```text
Medir
  ↓
Inspeccionar el Physical Plan
  ↓
Identificar shuffles, I/O y otros costes
  ↓
Formular una hipótesis
  ↓
Aplicar una optimización
  ↓
Volver a medir
```

El objetivo no es aplicar optimizaciones de forma sistemática, sino **comprender qué coste se está intentando reducir y comprobar experimentalmente si la modificación realmente mejora el workload**.