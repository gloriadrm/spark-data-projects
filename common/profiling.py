from pyspark.sql import functions as F
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


def duplicate_count(df):
    duplicates = (
        df.groupBy(*df.columns)
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicates.agg(
        # duplicate_groups = unique values de duplicated rows
        F.count("*").alias("duplicate_groups"),
        F.sum(F.col("count") - 1).alias("duplicate_rows")
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

def cardinality_profile(df):
    row_count = df.count()

    aggregations = [
        F.countDistinct(column).alias(column)
        for column in df.columns
    ]

    counts = df.agg(*aggregations)

    profile = []

    for column in df.columns:
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
