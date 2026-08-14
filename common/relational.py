from pyspark.sql import functions as F


from functools import reduce
from operator import or_
from pyspark.sql import functions as F

def primary_key_check(df, columns):
    if isinstance(columns, str):
        columns = [columns]

    null_condition = reduce(
        or_,
        [F.col(c).isNull() for c in columns]
    )

    result = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(null_condition, 1).otherwise(0)
        ).alias("rows_with_null_key"),
        F.countDistinct(
            F.struct(*[F.col(c) for c in columns])
        ).alias("distinct_keys"),
    )

    return (
        result
        .withColumn(
            "duplicate_keys",
            F.col("total_rows") - F.col("distinct_keys"),
        )
        .withColumn(
            "is_valid_primary_key",
            (F.col("rows_with_null_key") == 0)
            & (F.col("duplicate_keys") == 0),
        )
    )

def foreign_key_check(child_df, child_column, parent_df, parent_column):
    orphan_rows = (
        child_df
        .filter(F.col(child_column).isNotNull())
        .join(
            parent_df.select(
                F.col(parent_column).alias(child_column)
            ).distinct(),
            on=child_column,
            how="left_anti",
        )
    )

    return (
    orphan_rows.agg(
        F.count("*").alias("orphan_rows"),
        F.countDistinct(child_column).alias("orphan_keys"),
    )
    .withColumn(
        "is_valid_foreign_key",
        (F.col("orphan_rows") == 0) & (F.col("orphan_keys") == 0)
        )
    )