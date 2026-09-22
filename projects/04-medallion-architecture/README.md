# 04 · Medallion (Bronze → Silver → Gold)

## Objetivo

Implementar una arquitectura **Medallion (Bronze → Silver → Gold)** con PySpark sobre un dataset lo suficientemente grande como para trabajar de forma realista aspectos como el **particionado**, la **calidad del dato**, la **deduplicación** y la **escritura idempotente**.

El objetivo es construir un flujo de transformación progresivo desde los datos originales hasta datasets preparados para consumo analítico.

## Dataset

[Chicago Crimes](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

- Periodo: **2001–2026**
- Volumen: **~8,6 millones de registros**
- Tamaño del CSV: **~2,4 GB**

## Arquitectura

El procesamiento se organiza en tres capas:

```text
CSV (~2,4 GB / 8,6M filas)
        │
        ▼
     BRONZE
 Datos ingeridos con schema explícito
 y transformaciones mínimas
        │
        ▼
     SILVER
 Limpieza · tipado · deduplicación
 controles de calidad · tratamiento
 de anomalías
        │
        ▼
      GOLD
 Agregaciones y datasets preparados
 para consumo analítico
```

### Bronze

La capa Bronze conserva los datos procedentes de la fuente con transformaciones mínimas.

Se utiliza un **schema explícito** para evitar el coste y la incertidumbre asociados a la inferencia automática de tipos.

Los datos se almacenan en formato **Parquet**, manteniendo una representación próxima a la fuente original.

### Silver

La capa Silver contiene los datos limpios y preparados para ser reutilizados en diferentes análisis.

Las principales operaciones realizadas son:

- Conversión y normalización de tipos de datos.
- Transformación de campos temporales.
- Análisis y tratamiento de valores nulos.
- Identificación de duplicados técnicos y duplicados de negocio.
- Validación de información geoespacial.
- Aplicación de reglas de calidad.
- Selección y preparación de variables relevantes.
- Escritura particionada en Parquet.

### Gold

La capa Gold contiene datasets agregados orientados directamente al consumo analítico.

Se construyen datasets para responder a cuatro áreas principales:

1. **Evolución temporal de la criminalidad.**
2. **Distribución de delitos por distrito.**
3. **Tipología delictiva.**
4. **Tasa de arrestos global y por tipo de delito.**

## Aspectos trabajados

- Arquitectura **Medallion: Bronze → Silver → Gold**.
- Procesamiento distribuido con **PySpark**.
- Definición explícita de schemas.
- Profiling y análisis de calidad de datos.
- Limpieza y transformación de datos.
- Deduplicación basada en criterios técnicos y de negocio.
- Validación de anomalías geoespaciales.
- Transformaciones y agregaciones con Spark.
- Almacenamiento en **Parquet**.
- Particionado físico de datasets.
- Escrituras idempotentes y reprocesamiento seguro.
- Construcción de datasets Gold orientados a KPIs y análisis.

## Resultados

Se ha implementado y validado el flujo completo **Bronze → Silver → Gold** sobre aproximadamente **8,6 millones de registros**.

En Bronze se mantienen los datos próximos a la fuente original y se utiliza un schema explícito para controlar los tipos desde la ingesta.

En Silver se realizan las operaciones de limpieza, tipado, deduplicación y validación necesarias para obtener un dataset fiable y reutilizable.

Finalmente, en Gold se generan datasets agregados orientados al análisis de la evolución temporal, distribución geográfica, tipología delictiva y tasas de arresto.

El pipeline se ha diseñado teniendo en cuenta tanto el **particionado de los datos** como el **reprocesamiento mediante escrituras idempotentes**.

## Lecciones aprendidas

- En las pruebas realizadas sobre este dataset, `inferSchema=True` incrementó el tiempo de lectura inicial de aproximadamente **0,05 s con schema explícito a 6,3 s con inferencia automática**. Además del coste de inferencia, declarar el schema hace el pipeline más predecible y permite controlar explícitamente los tipos de datos.

- La **cardinalidad** puede ocultar problemas de calidad incluso cuando una columna parece prácticamente única. `Case Number` presentaba una unicidad cercana al 100 %, pero investigar la pequeña diferencia permitió detectar **522 Case Number repetidos correspondientes a 1.148 registros**.

- La definición de duplicado depende del significado de los datos. Al excluir `ID` de la comparación se identificaron **156 grupos de duplicados de negocio**, mostrando que una clave técnica diferente no implica necesariamente que dos registros representen sucesos distintos.

- Dos anomalías geoespaciales que inicialmente parecían independientes —**149 registros con coordenadas X/Y iguales a 0** y **149 registros con `Location` fuera del área esperada de Chicago**— correspondían exactamente al mismo conjunto de filas. Comparar las poblaciones afectadas permitió evitar tratar dos síntomas del mismo problema como incidencias independientes.

- Las métricas agregadas pueden ocultar una gran heterogeneidad. La tasa de arrestos cambia considerablemente según el tipo de delito: entre los tipos analizados, pasa del **17,66 % en `OTHER OFFENSE`** al **99,55 % en `PROSTITUTION`** y **99,31 % en `NARCOTICS`**.

## Próximos pasos

Como evolución del proyecto, la lógica actualmente validada en el notebook puede modularizarse en un pipeline ejecutable mediante componentes independientes para las capas **Bronze, Silver y Gold**, junto con un módulo específico para los **controles de calidad**.

Esto permitiría separar la fase exploratoria del código de producción y facilitar la ejecución, mantenimiento y reutilización del pipeline.
