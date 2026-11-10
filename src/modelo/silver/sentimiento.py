"""Silver - sentimiento: CSV crudo (todo texto) -> Fecha (date), Sector normalizado, Sentimiento (double).

Usado en: Ej8.
"""
from pyspark.sql import functions as F


# Usado en: Ej8
def limpiar_sentimiento(df):
    """Fecha a date, Sector sin espacios y con mayúscula inicial ("Financial Services", como en Yahoo),
    Sentimiento a double (cambiando la coma decimal por punto). Quita nulos y duplicados por (Fecha, Sector)."""
    return (df
            .withColumn("Fecha", F.to_date("Fecha"))
            .withColumn("Sector", F.initcap(F.trim(F.col("Sector"))))
            .withColumn("Sentimiento", F.regexp_replace("Sentimiento", ",", ".").cast("double"))
            .dropna()
            .dropDuplicates(["Fecha", "Sector"]))