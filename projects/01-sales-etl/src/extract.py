"""Extraccion de datos crudos para 01-sales-etl. Nunca transforma, solo entrada de datos."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from common.spark_session import get_spark

PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"


def read_raw():
    spark = get_spark(app_name="01-sales-etl-extract")
    # TODO: spark.read.csv(str(DATA_DIR / "..."), header=True, inferSchema=False, schema=...)
    raise NotImplementedError


if __name__ == "__main__":
    read_raw()
