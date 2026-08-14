"""Escritura de resultados de 01-sales-etl."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from common.utils import write_parquet

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "output"


def write_cleaned_sales(df):
    write_parquet(df, OUTPUT_DIR / "cleaned_sales")


def write_sales_by_month(df):
    write_parquet(df, OUTPUT_DIR / "sales_by_month")


def write_sales_by_category(df):
    write_parquet(df, OUTPUT_DIR / "sales_by_category")


def write_top_products(df):
    write_parquet(df, OUTPUT_DIR / "top_products")


if __name__ == "__main__":
    pass
