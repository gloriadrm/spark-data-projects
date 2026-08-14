"""Transformaciones principales de 01-sales-etl."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from extract import read_raw


def clean(df):
    """Tipado, nulos, duplicados, normalizacion de nombres de columna."""
    # TODO
    raise NotImplementedError


def enrich(df):
    """Columnas derivadas (withColumn: total_price, etc.) y joins con datasets auxiliares."""
    # TODO
    raise NotImplementedError


def transform():
    df = read_raw()
    df = clean(df)
    df = enrich(df)
    return df


if __name__ == "__main__":
    transform()
