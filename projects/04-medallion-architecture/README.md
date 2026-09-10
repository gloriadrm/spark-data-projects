# 04 · Medallion (Bronze → Silver → Gold)

# Objetivo

# Dataset
Reutilizar el dataset de 01-sales-etl o 02-nyc-taxi, esta vez con foco en arquitectura.

## Qué practico

- particionado de Parquet
- separación Bronze (crudo) / Silver (limpio) / Gold (agregado, listo para consumo)
- buenas prácticas de escritura idempotente

# Resultado esperado

Pipeline con tres capas físicas en `output/bronze`, `output/silver`, `output/gold`, cada una particionada donde tenga sentido.

## Lecciones aprendidas
