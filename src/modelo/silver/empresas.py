"""Silver - empresas: bronze/empresas -> dimensión limpia con el sector también en español.

Usado en: Ej9.
"""
from pyspark.sql import functions as F

from src.modelo.comun.config import SECTORES_ES


# Usado en: Ej9
def limpiar_empresas(df):
    """Ej9: info de Yahoo (bronze/empresas) -> dimensión limpia con nombres en español.

    Textos sin espacios y vacíos como nulo, Sector con el formato de Yahoo, recomendación "none" como
    nulo, un único registro por Ticker y el nombre del sector en español (Sector_ES).
    """
    def texto(c):
        return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))

    recomendacion = F.lower(texto("recommendationKey"))
    limpio = df.select(
        F.upper(F.trim(F.col("Ticker"))).alias("Ticker"),
        texto("longName").alias("Nombre"),
        F.initcap(F.lower(texto("sector"))).alias("Sector"),
        texto("industry").alias("Industria"),
        texto("country").alias("Pais"),
        F.upper(texto("currency")).alias("Moneda"),
        texto("exchange").alias("Bolsa"),
        F.col("marketCap").alias("Capitalizacion"),
        F.col("beta").alias("Beta"),
        F.when(recomendacion == "none", F.lit(None)).otherwise(recomendacion).alias("Recomendacion"),
        F.col("recommendationMean").alias("Recomendacion_Media"),
        F.col("numberOfAnalystOpinions").alias("Num_Analistas"),
        F.col("targetMeanPrice").alias("Precio_Objetivo"),
        F.col("FechaExtraccion"),
    ).dropna(subset=["Ticker"]).dropDuplicates(["Ticker"])
    es = df.sparkSession.createDataFrame(list(SECTORES_ES.items()), "Sector string, Sector_ES string")
    return (limpio.join(F.broadcast(es), "Sector", "left")
            .select("Ticker", "Nombre", "Sector", "Sector_ES", "Industria", "Pais", "Moneda", "Bolsa",
                    "Capitalizacion", "Beta", "Recomendacion", "Recomendacion_Media", "Num_Analistas",
                    "Precio_Objetivo", "FechaExtraccion"))
