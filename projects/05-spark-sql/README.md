# 05 · Spark SQL

## 🎯 Objetivo

Resolver las mismas transformaciones mediante **DataFrame API y Spark SQL** para analizar si ambas interfaces generan planes de ejecución equivalentes y entender cómo interviene **Catalyst Optimizer** independientemente de la sintaxis utilizada.

El objetivo no es únicamente practicar SQL en Spark, sino comprobar qué ocurre internamente desde la consulta escrita por el desarrollador hasta el **Physical Plan** ejecutado por Spark.

---

## 📦 Dataset

Se utiliza **Chicago Crimes**, el mismo dataset del proyecto 04, con más de **8,6 millones de registros**.

En este proyecto no se reproduce el pipeline Medallion completo. Se aplica únicamente la preparación necesaria para realizar las comparaciones:

- Schema explícito.
- Normalización de nombres de columnas a snake_case.
- Conversión de `date` al tipo temporal correspondiente.

---

## 🧪 Comparaciones realizadas

Se implementan cuatro casos con **DataFrame API** y **Spark SQL**:

1. **Filtro + selección**
2. **Agregación**
3. **Agregación con cálculo de una métrica**
4. **Top N por grupo mediante Window Function + CTE**

Para cada caso se comparan:

- Resultado obtenido.
- Implementación mediante ambas APIs.
- Legibilidad.
- Logical Plan.
- Physical Plan generado mediante `explain()`.

---

## 🧠 Conceptos practicados

- [x] `createOrReplaceTempView()`
- [x] Consultas mediante `spark.sql()`
- [x] Equivalencias SQL ↔ DataFrame API
- [x] `filter`, `select`, `groupBy` y `agg`
- [x] CTEs
- [x] Window Functions
- [x] `explain()`
- [x] Logical Plan vs Physical Plan
- [x] Catalyst Optimizer
- [x] Predicate pushdown
- [x] Column pruning
- [x] `WindowGroupLimit`
- [x] Comparación de planes de ejecución

---

## 🔎 Resultados

En **3 de las 4 comparaciones** se generó el mismo Physical Plan.

La única diferencia apareció en el cálculo de la **tasa de arrestos**. En DataFrame API el cálculo se realizó mediante un `.withColumn()` posterior al `.agg()`, generando un `Project` adicional, mientras que Spark SQL integró el cálculo directamente dentro del `HashAggregate`.

A pesar de esta diferencia estructural, ambos planes mantuvieron el mismo número de operaciones `Exchange` y no se observó una diferencia relevante en el coste de ejecución.

---

## ⚙️ Optimizaciones observadas

El análisis de los planes permitió observar optimizaciones aplicadas automáticamente por **Catalyst Optimizer**:

### Predicate pushdown

Los filtros pueden desplazarse hacia la lectura del dataset, apareciendo reflejados en `PushedFilters` dentro del `FileScan`.

Esto permite reducir los datos procesados lo antes posible.

### Column pruning

Aunque el CSV original contiene 21 columnas, Spark evita mantener columnas que no son necesarias para resolver una consulta.

El `FileScan` muestra únicamente las columnas requeridas por cada operación.

### WindowGroupLimit

En el patrón utilizado para obtener un **Top N por grupo**, Spark introduce `WindowGroupLimit` para limitar los registros necesarios durante el procesamiento de la Window Function.

---

## 💡 Lecciones aprendidas

### DataFrame API y Spark SQL son dos interfaces sobre el mismo motor

Escribir una transformación mediante DataFrame API o mediante SQL no implica necesariamente una estrategia de ejecución diferente.

Ambas representaciones terminan siendo procesadas por Catalyst, que genera y optimiza el plan que finalmente ejecutará Spark.

### Una diferencia de sintaxis no implica una diferencia relevante de rendimiento

Dos consultas pueden producir planes con pequeñas diferencias estructurales —por ejemplo, un `Project` adicional— sin modificar las operaciones realmente costosas, como los shuffles (`Exchange`).

Por tanto, comparar únicamente la apariencia del plan puede llevar a conclusiones incorrectas.

### Las consultas deben ser semánticamente equivalentes antes de comparar sus planes

En la comparación más compleja, los planes SQL y DataFrame inicialmente no coincidían.

La causa no era una diferencia entre las APIs: a la consulta SQL le faltaba el `ORDER BY` final presente en la versión DataFrame.

Después de igualar ambas consultas, los Physical Plans coincidieron.

**Antes de atribuir una diferencia de ejecución a SQL o DataFrame API, hay que comprobar que ambas consultas describen exactamente la misma operación.**

---

## ✅ Conclusión

El proyecto muestra que **Spark SQL y DataFrame API pueden expresar las mismas transformaciones y, en muchos casos, terminar generando el mismo Physical Plan**.

La elección entre ambas APIs puede realizarse principalmente en función de la legibilidad, mantenibilidad y contexto de uso, sin asumir que una de ellas será automáticamente más eficiente.

El análisis mediante `explain()` permite ir más allá de la sintaxis y entender cómo Spark optimiza realmente las transformaciones antes de ejecutarlas.