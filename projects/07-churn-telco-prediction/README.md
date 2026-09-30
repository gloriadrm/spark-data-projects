# 07 · Customer Churn Prediction con Spark MLlib

## 🎯 Objetivo

Construir un pipeline de clasificación con **Spark MLlib** que estime la probabilidad de que un cliente de telecomunicaciones abandone el servicio (*churn*).

**Pregunta de negocio:** ¿qué clientes tienen mayor probabilidad de abandonar el servicio y qué factores se asocian a ese abandono?

El objetivo no es solo entrenar un modelo, sino recorrer el proceso completo con criterio: calidad de los datos, preparación de variables, evitar *data leakage*, elegir la métrica adecuada para un problema desbalanceado, fijar el umbral de decisión e interpretar el resultado.

---

## 📦 Dataset

**Telco Customer Churn** (IBM), en `datasets/05-telco-customer-churn`.

- **7.043 clientes** · 21 columnas · CSV de ~1 MB
- Datos demográficos, servicios contratados, tipo de contrato, método de pago y facturación
- Variable objetivo: `Churn` (Yes/No), clientes que se dieron de baja en el último mes
- **Desbalanceo moderado:** 26,6 % de churn

---

## 🔄 Pipeline

```text
CSV ──► limpieza (df_clean) ──► codificación para el modelo (df_model)
                                        │
                        split estratificado 80/20 (train / test)
                                        │
         Pipeline: StringIndexer → OneHotEncoder → VectorAssembler → [StandardScaler] → modelo
                                        │
            evaluación ──► umbral de decisión ──► interpretabilidad ──► predicciones
```

---

## 🧹 Calidad de los datos y decisiones de limpieza

| Problema detectado | Decisión |
|---|---|
| `TotalCharges` inferida como texto por 11 valores en blanco | Los 11 corresponden a clientes con `tenure = 0`, cuya etiqueta no es fiable (no han tenido tiempo de abandonar). Se eliminan: 7.043 → 7.032 clientes |
| `No internet service` / `No phone service` repiten en 7 columnas información ya recogida en `InternetService` y `PhoneService` | Se recodifican como `No`: las 7 columnas pasan a ser binarias sin pérdida de información |
| 42 clientes con perfil idéntico (excluyendo el identificador) | Coincidencias legítimas: `customerID` es único. Se conservan |
| Nombres de columna con estilos mezclados | Normalizados a `snake_case` tras la lectura |

La limpieza produce `df_clean` (valores legibles, Yes/No) y la preparación para el modelo produce `df_model` (binarias 0/1, `gender_male`, `label`). Separarlos permite reejecutar cualquier celda sin transformar dos veces la misma columna.

---

## 🔎 Factores asociados al churn

| Factor | Tasa de churn |
|---|---|
| Antigüedad 0–12 meses / más de 48 meses | 47,7 % / 9,5 % |
| Contrato mensual / de dos años | 42,7 % / 2,9 % |
| Fibra óptica / DSL / sin internet | 41,9 % / 19,0 % / 7,4 % |
| Electronic check / resto de métodos de pago | 45,3 % / 15–19 % |

La exploración, los coeficientes de la regresión logística y la importancia de features del Random Forest coinciden en estos factores. Son asociaciones, no relaciones causales.

---

## 🤖 Modelos

Los tres modelos comparten el mismo preprocesado dentro de un `Pipeline`, ajustado solo con train.

| Modelo | AUC-ROC | AUC-PR | Accuracy | Precision (churn) | Recall (churn) | F1 (churn) |
|---|---:|---:|---:|---:|---:|---:|
| **Regresión logística** | **0,861** | **0,664** | **0,815** | 0,633 | 0,601 | **0,617** |
| Árbol de decisión | 0,822 | 0,638 | 0,800 | 0,592 | **0,619** | 0,605 |
| Random Forest | 0,851 | 0,658 | 0,813 | **0,682** | 0,457 | 0,547 |

*Métricas sobre el conjunto de test (1.327 clientes) con umbral 0,5.*

**Modelo seleccionado: regresión logística.** El churn depende de pocos factores con una relación bastante directa, que un modelo lineal captura bien. Los modelos más complejos no aportan mejora.

---

## 🎚️ Umbral de decisión

| Umbral | Precision | Recall | F1 |
|---:|---:|---:|---:|
| 0,3 | 0,521 | 0,823 | 0,638 |
| **0,4** | 0,580 | 0,717 | **0,641** |
| 0,5 | 0,633 | 0,601 | 0,617 |

Bajar el umbral aumenta el recall con **rendimiento decreciente**: cada churner adicional detectado cuesta 1,5 falsas alarmas al pasar de 0,5 a 0,4, y 5,5 al pasar de 0,3 a 0,2.

Se elige **0,4** como umbral equilibrado: detecta el **72 % de los churners** frente al 60 % con 0,5. Si una acción de retención es barata comparada con perder un cliente, 0,3 detecta el 82 %.

---

## ⚖️ Spark MLlib frente a scikit-learn

La misma regresión logística con scikit-learn, con el mismo split y un preprocesado equivalente:

| | Spark MLlib | scikit-learn |
|---|---:|---:|
| AUC-ROC | 0,861 | 0,861 |
| F1 (churn) | 0,617 | 0,614 |
| Tiempo de entrenamiento | 7,1 s | 0,1 s |

Métricas equivalentes (diferencias < 0,003). scikit-learn entrena más de un orden de magnitud más rápido: con datos que caben en memoria, el coste de coordinación de Spark no compensa. Spark MLlib aporta valor cuando el volumen supera la capacidad de una máquina o los datos ya residen en un entorno distribuido.

---

## 📊 Resultados

```text
output/
├── predictions/          # Parquet: clientes de test ordenados por probabilidad de churn
└── model_metrics.json    # modelo y umbral seleccionados, métricas y tiempos de cada modelo
```

`predictions/` contiene `customer_id`, `contract`, `tenure`, `monthly_charges`, `churn_probability`, `prediction` (umbral 0,4) y `label`. De los 20 clientes con mayor riesgo, 17 abandonaron realmente.

El análisis completo, con todas las salidas y decisiones, está en `notebook.ipynb`.

---

## ⚠️ Limitaciones y próximos pasos

**Limitaciones**

- El umbral se eligió sobre el conjunto de test; lo correcto sería elegirlo con datos de validación.
- **Multicolinealidad:** `total_charges ≈ tenure × monthly_charges`. Los coeficientes de estas variables no se interpretan por separado (por ejemplo, `monthly_charges` sale con signo negativo aunque los churners pagan cuotas más altas).
- Modelos entrenados con hiperparámetros fijos.
- El dataset es una foto de un momento concreto; no recoge la evolución del cliente en el tiempo.

**Próximos pasos**

- Ajustar hiperparámetros y umbral con `CrossValidator` + `ParamGridBuilder`.
- Evaluar un modelo sin `total_charges` para reducir la redundancia.
- Probar ponderación de clases (`weightCol`) como alternativa al ajuste del umbral.
- Traducir el umbral a un criterio de coste de negocio.

---

## ✅ Checklist de conceptos

- [x] Lectura de CSV con `inferSchema` y sus riesgos
- [x] Perfilado de datos: duplicados, nulos y valores vacíos disfrazados, cardinalidad
- [x] `try_cast` y modo ANSI de Spark 4
- [x] Split train/test estratificado y `cache()`
- [x] Data leakage y ajuste del preprocesado solo con train
- [x] `StringIndexer`, `OneHotEncoder` (`handleInvalid`, `dropLast`), `VectorAssembler`
- [x] Vectores dispersos (*sparse vectors*)
- [x] `StandardScaler` y coeficientes comparables
- [x] `Pipeline` y `PipelineModel`
- [x] Regresión logística, árbol de decisión y Random Forest
- [x] `BinaryClassificationEvaluator` y `MulticlassClassificationEvaluator`
- [x] Matriz de confusión, precision, recall, F1, AUC-ROC y AUC-PR
- [x] Umbral de decisión
- [x] Coeficientes, odds ratio e importancia de features
- [x] Multicolinealidad
- [ ] `CrossValidator` y `ParamGridBuilder`

---

## 💡 Lecciones aprendidas

- **"Sin nulos" no significa "sin datos ausentes".** Los valores vacíos pueden llegar como texto con espacios; `inferSchema` los delata al inferir como texto una columna numérica.
- **Entender por qué falta un dato antes de decidir qué hacer con él.** Los 11 clientes sin facturación no eran un error, pero su etiqueta no era fiable.
- **Detectar información redundante mirando los recuentos.** Que una categoría aparezca exactamente el mismo número de veces en varias columnas es una pista de que repiten información.
- **Todo lo que aprende de los datos se ajusta solo con train.** Encapsular preprocesado y modelo en un `Pipeline` lo garantiza.
- **Con clases desbalanceadas, accuracy engaña.** Predecir siempre "no abandona" ya da un 73 %; lo relevante es el recall y la AUC-PR de la clase positiva.
- **La AUC mide cómo ordena el modelo; el umbral decide.** Dos modelos con AUC parecida pueden tener recalls muy distintos con el mismo umbral.
- **En el árbol de decisión, `rawPrediction` no sirve para calcular la AUC:** son recuentos de la hoja sin normalizar. Hay que usar `probability`.
- **El umbral es una decisión de negocio**, y también un hiperparámetro: debe elegirse con datos de validación.
- **Más complejo no es mejor.** La regresión logística superó al Random Forest porque el problema se explica con pocos factores directos.
- **Con variables correlacionadas, los coeficientes no se interpretan por separado.**
- **Spark no es la herramienta para todo:** con datos pequeños, scikit-learn da el mismo resultado mucho más rápido.
