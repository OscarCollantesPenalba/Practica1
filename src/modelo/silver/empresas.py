"""Silver - empresas: bronze/empresas -> dimensión limpia, con el sector también en español.

Usado en: Ej9.
"""
from pyspark.sql import functions as F

from src.modelo.comun.config import SECTORES_ES


# Usado en: Ej9
def limpiar_empresas(df):
    """Columnas con nombres claros, Sector con el formato del sentimiento (cruce del Ej10),
    un único registro por Ticker y el sector en español (Sector_ES)."""
    limpio = df.select(
        "Ticker",
        F.col("longName").alias("Nombre"),
        F.initcap(F.trim(F.col("sector"))).alias("Sector"),
        F.col("industry").alias("Industria"),
        F.col("country").alias("Pais"),
        F.col("marketCap").alias("Capitalizacion"),
        F.col("beta").alias("Beta"),
        F.col("recommendationMean").alias("Recomendacion_Media"),
        F.col("numberOfAnalystOpinions").alias("Num_Analistas"),
        F.col("targetMeanPrice").alias("Precio_Objetivo"),
        "FechaExtraccion",
    ).dropDuplicates(["Ticker"])
    sectores_es = df.sparkSession.createDataFrame(list(SECTORES_ES.items()), ["Sector", "Sector_ES"])
    return (limpio.join(sectores_es, "Sector", "left")
            .select("Ticker", "Nombre", "Sector", "Sector_ES", "Industria", "Pais", "Capitalizacion",
                    "Beta", "Recomendacion_Media", "Num_Analistas", "Precio_Objetivo", "FechaExtraccion"))