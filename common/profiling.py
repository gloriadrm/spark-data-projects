from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StringType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)


def duplicate_count(df, exclude_columns=None):
    exclude_columns = exclude_columns or []

    columns = [
        column
        for column in df.columns
        if column not in exclude_columns
    ]

    if not columns:
        raise ValueError(
            "Debe quedar al menos una columna para comprobar duplicados."
        )

    duplicates = (
        df
        .groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicates.agg(
        # Número de patrones distintos que aparecen más de una vez
        F.count("*").alias("duplicate_groups"),

        # Número de filas redundantes si conservamos una copia de cada patrón
        F.coalesce(
            F.sum(F.col("count") - 1),
            F.lit(0)
        ).alias("duplicate_rows")
    )

def missing_values_profile(
    df,
    string_markers=None,
    output="count",
):
    """
    Profile each representation of missing data by column.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame to inspect.

    string_markers : dict, optional
        Mapping between output column names and textual markers.

        Example:
        {
            "na_count": "NA",
            "n_a_count": "N/A",
            "unknown_count": "Unknown",
            "empty_count": ""
        }

    output : str, default="count"
        Output format:

        - "count": absolute values.
        - "percentage": percentage of total DataFrame rows.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per column and one metric per missing representation.
    """

    valid_outputs = {"count", "percentage"}

    if output not in valid_outputs:
        raise ValueError(
            f"output must be one of {valid_outputs}. "
            f"Received: {output!r}"
        )

    string_markers = string_markers or {}

    aggregations = []

    for field in df.schema.fields:
        column_name = field.name
        column = F.col(column_name)

        # NULL: válido para cualquier tipo
        aggregations.append(
            F.count(
                F.when(column.isNull(), 1)
            ).alias(f"{column_name}__null_count")
        )

        # NaN: solo para float y double
        if isinstance(field.dataType, (FloatType, DoubleType)):
            aggregations.append(
                F.count(
                    F.when(F.isnan(column), 1)
                ).alias(f"{column_name}__nan_count")
            )

        # Marcadores textuales: solo para columnas string
        if isinstance(field.dataType, StringType):
            for count_name, marker in string_markers.items():
                aggregations.append(
                    F.count(
                        F.when(column == marker, 1)
                    ).alias(f"{column_name}__{count_name}")
                )

    counts_row = df.select(*aggregations)

    profile_rows = []

    for field in df.schema.fields:
        column_name = field.name

        expressions = [
            F.lit(column_name).alias("column_name"),
            F.col(f"`{column_name}__null_count`").alias("null_count"),
        ]

        if isinstance(field.dataType, (FloatType, DoubleType)):
            expressions.append(
                F.col(f"`{column_name}__nan_count`").alias("nan_count")
            )
        else:
            expressions.append(
                F.lit(0).cast("long").alias("nan_count")
            )

        for count_name in string_markers:
            alias = f"{column_name}__{count_name}"

            if isinstance(field.dataType, StringType):
                expressions.append(
                    F.col(f"`{alias}`").alias(count_name)
                )
            else:
                expressions.append(
                    F.lit(0).cast("long").alias(count_name)
                )

        profile_rows.append(
            counts_row.select(*expressions)
        )

    result = profile_rows[0]

    for row_df in profile_rows[1:]:
        result = result.unionByName(row_df)

    count_columns = [
        "null_count",
        "nan_count",
        *string_markers.keys(),
    ]

    total_expression = sum(
        (F.col(column) for column in count_columns),
        start=F.lit(0),
    )

    result = (
        result
        .withColumn("missing_total", total_expression)
        .filter(F.col("missing_total") > 0)
    )

    if output == "percentage":
        row_count = df.count()

        if row_count == 0:
            return result

        metric_columns = [
            *count_columns,
            "missing_total",
        ]

        for column_name in metric_columns:
            result = result.withColumn(
                column_name,
                F.round(
                    F.col(column_name) / F.lit(row_count) * 100,
                    2,
                ),
            )

    return result.orderBy(
        F.col("missing_total").desc()
    )

def show_result(df, empty_message):
    if df.isEmpty():
        print(empty_message)
    else:
        df.show(truncate=False)

def cardinality_profile(df, columns=None):
    if columns is None:
        columns = df.columns

    if not columns:
        raise ValueError("La lista de columnas no puede estar vacía.")

    row_count = df.count()

    aggregations = [
        F.countDistinct(column).alias(column)
        for column in columns
    ]

    counts = df.agg(*aggregations)

    profile = []

    for column in columns:
        profile.append(
            counts.select(
                F.lit(column).alias("column_name"),
                F.col(f"`{column}`").alias("distinct_values"),
                F.round(
                    F.col(f"`{column}`") / F.lit(row_count) * 100,
                    2,
                ).alias("cardinality_pct"),
            )
        )

    result = profile[0]

    for row in profile[1:]:
        result = result.unionByName(row)

    return result.orderBy(F.col("distinct_values").desc())

def frequency_profile(df, column, limit=None):
    total_rows = df.count()

    result = (
        df
        .groupBy(
            F.coalesce(
                F.col(f"`{column}`").cast("string"),
                F.lit("NULL")
            ).alias("category")
        )
        .count()
        .withColumn(
            "percentage",
            F.round(
                F.col("count") / F.lit(total_rows) * 100,
                2
            )
        )
        .orderBy(F.col("count").desc())
    )

    if limit is not None:
        result = result.limit(limit)

    return result

def numeric_summary (df):
    numeric_types = (
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType,
    )

    numeric_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, numeric_types)
    ]

    if not numeric_columns:
        return None

    summary_df = df.select(numeric_columns).summary()

    return summary_df

def quantile_summary(
    df,
    columns: list[str],
    percentiles: list[float] | None = None,
    relative_error: float = 0.001,
):
    """
    Calcula percentiles aproximados para un conjunto de columnas numéricas.

    Args:
        df: DataFrame de Spark.
        columns: Columnas numéricas a analizar.
        percentiles: Lista de percentiles (entre 0 y 1).
        relative_error: Error relativo permitido por approxQuantile().

    Returns:
        DataFrame de Spark con una fila por variable.
    """

    if percentiles is None:
        percentiles = [0.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999]

    rows = []

    for col in columns:
        values = df.approxQuantile(
            col,
            percentiles,
            relative_error,
        )

        rows.append(
            Row(
                variable=col,
                min=values[0],
                p25=values[1],
                median=values[2],
                p75=values[3],
                p90=values[4],
                p95=values[5],
                p99=values[6],
                p999=values[7],
            )
        )

    return df.sparkSession.createDataFrame(rows)