"""Silver - precios: bronze/ibex -> precios limpios, tipados y sin duplicados.

Usado en: Ej9.
"""
from pyspark.sql import functions as F


# Usado en: Ej9
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
