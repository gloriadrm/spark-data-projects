"""Utilidades comunes: guardado de resultados, inspeccion rapida de DataFrames."""
from pathlib import Path

from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, path: Path, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(str(path))


def show_schema_and_sample(df: DataFrame, n: int = 5) -> None:
    df.printSchema()
    df.show(n, truncate=False)
