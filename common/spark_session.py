"""Sesion de Spark reutilizable para todos los mini proyectos."""
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "spark-lab", shuffle_partitions: int = 4) -> SparkSession:
    """Crea (o reutiliza) una SparkSession local para desarrollo.
    Args:
        app_name: nombre de la aplicacion, util para identificarla en la Spark UI.
        shuffle_partitions: particiones tras un shuffle. Bajo en local porque
            el valor por defecto de Spark (200) sobra para datasets pequenos.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]") 
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
