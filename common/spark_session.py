"""Sesion de Spark reutilizable para todos los mini proyectos."""

import os
import sys

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "spark-lab",
    shuffle_partitions: int = 4,
) -> SparkSession:
    """Crea (o reutiliza) una SparkSession local para desarrollo."""

    # Fuerza a Spark a utilizar el mismo Python que el notebook
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )