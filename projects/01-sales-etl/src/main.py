"""Orquestacion de 01-sales-etl: extract -> transform -> quality -> analysis -> load."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from common.utils import show_schema_and_sample
from extract import read_raw
from transform import clean, enrich
from quality import validate
from analysis import monthly_sales, sales_by_category, top_products
from load import write_cleaned_sales, write_sales_by_month, write_sales_by_category, write_top_products


def main():
    df = read_raw()
    df = clean(df)
    df = enrich(df)
    validate(df)
    show_schema_and_sample(df)

    write_cleaned_sales(df)
    write_sales_by_month(monthly_sales(df))
    write_sales_by_category(sales_by_category(df))
    write_top_products(top_products(df))

    # TODO: exponer tambien total_sales(df), sales_by_region(df), average_ticket(df)
    # segun se necesiten en el notebook / informe final.


if __name__ == "__main__":
    main()
