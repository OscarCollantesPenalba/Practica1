"""Ingesta del CSV de sentimiento por sector."""
from src.modelo.config import CSV_SENTIMIENTO


def leer_csv_sentimiento(spark, ruta=None):
    """Ej2a: lee el CSV tal cual. Sin inferSchema, un CSV no guarda tipos: todo es string."""
    ruta = (ruta or CSV_SENTIMIENTO)
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(str(ruta).replace("\\", "/")))
