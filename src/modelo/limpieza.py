"""Limpieza de bronze hacia silver: tipos correctos, valores normalizados y sin duplicados.

Spark 4 trabaja con el modo ANSI activado: un cast o to_date sobre un valor mal formado lanza
error. Por eso se usa try_cast, que devuelve nulo y permite descartar esas filas de forma controlada.
"""
from pyspark.sql import functions as F

from src.modelo.config import SECTORES_ES


def limpiar_sentimiento(df):
    """Ej8: sentimiento crudo (todo texto) -> Fecha (date), Sector (normalizado), Sentimiento (double).

    - Fecha: texto -> date.
    - Sector: sin espacios sobrantes y con el formato de Yahoo ("Financial Services"), para poder
      cruzarlo con el sector de cada empresa.
    - Sentimiento: texto -> double, aceptando la coma como separador decimal.
    - Se descartan las filas con algún campo nulo y los duplicados por (Fecha, Sector).
    """
    fecha = F.expr("try_cast(trim(Fecha) as date)")
    sector = F.initcap(F.lower(F.trim(F.col("Sector"))))
    sentimiento = F.expr("try_cast(regexp_replace(trim(Sentimiento), ',', '.') as double)")
    limpio = df.select(fecha.alias("Fecha"), sector.alias("Sector"), sentimiento.alias("Sentimiento"))
    return (limpio
            .dropna(subset=["Fecha", "Sector", "Sentimiento"])
            .filter(F.col("Sector") != "")
            .dropDuplicates(["Fecha", "Sector"]))


def limpiar_precios(df):
    """Ej9: precios de bronze -> silver.

    - Date se renombra a Fecha (misma clave que el sentimiento).
    - Los precios y dividendos se redondean a 4 decimales: Yahoo los entrega en float32 y al pasarlos
      a double aparece ruido (9.300000190734863 en lugar de 9.3).
    - Se descartan las sesiones sin precios completos, con Close <= 0 o con High < Low, y los
      duplicados por (Ticker, Fecha). Volume puede ser nulo.
    """
    precios = ["Open", "High", "Low", "Close"]
    limpio = df.select(
        F.col("Date").alias("Fecha"),
        F.upper(F.trim(F.col("Ticker"))).alias("Ticker"),
        *[F.round(F.col(c), 4).alias(c) for c in precios],
        F.col("Volume"),
        F.round(F.col("Dividends"), 4).alias("Dividends"),
        F.col("Stock_Splits"),
    )
    return (limpio
            .dropna(subset=["Fecha", "Ticker"] + precios)
            .filter((F.col("Close") > 0) & (F.col("High") >= F.col("Low")))
            .dropDuplicates(["Ticker", "Fecha"]))


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
