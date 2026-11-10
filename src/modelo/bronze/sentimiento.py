"""Bronze - sentimiento: lectura del CSV del analista, tal cual (todo texto).

Usado en: Ej2.
"""
from src.modelo.comun.config import CSV_SENTIMIENTO


# Usado en: Ej2a, Ej2b, Ej2c
def leer_csv_sentimiento(spark):
    """Lee el CSV tal cual. Sin inferSchema, un CSV no guarda tipos: todo llega como string."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(CSV_SENTIMIENTO))