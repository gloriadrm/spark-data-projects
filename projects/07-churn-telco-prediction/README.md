# 07 · Customer Churn Prediction with Spark MLlib

# Objetivo

Construir una canalizacion de clasificacion distribuida con Spark MLlib para estimar
la probabilidad de abandono (churn) de clientes de telecomunicaciones.

# Dataset

`datasets/05-telco-customer-churn` (Telco Customer Churn).

# Preguntas que responder

¿Que clientes presentan mayor probabilidad de abandonar el servicio?

# Transformaciones previstas

- Limpieza y preparacion de variables.
- `StringIndexer` para atributos categoricos.
- `OneHotEncoder` sobre los indices.
- `VectorAssembler` para construir el vector de caracteristicas.
- Encapsulacion de todo el preprocesado + modelo en un `Pipeline`.
- Entrenamiento y evaluacion de Regresion logistica, Arbol de decision y Random Forest.
- Comparacion con la misma solucion en scikit-learn (diseño de API, pipelines,
  metricas, interpretabilidad, escalabilidad).

# Resultado esperado

```
output/
├── predictions/
├── model_metrics.json
└── sklearn_vs_mllib.md
```

## Lecciones aprendidas

_(rellenar al terminar el proyecto)_
