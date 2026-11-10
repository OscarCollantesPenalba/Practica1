from pyspark.sql import functions as F


# Usado en: Ej9
def limpiar_precios(df):
    """- Date se renombra a Fecha (misma clave que el sentimiento).
    - Precios y dividendos a 4 decimales: Yahoo los entrega en float32 y al pasarlos a double aparece
      ruido (9.300000190734863 en lugar de 9.3).
    - Se descartan las sesiones sin precios completos, con Close <= 0 o con High < Low, y los
      duplicados por (Ticker, Fecha). Volume puede ser nulo."""
    precios = ["Open", "High", "Low", "Close"]
    limpio = df.withColumnRenamed("Date", "Fecha")
    for c in precios + ["Dividends"]:
        limpio = limpio.withColumn(c, F.round(c, 4))
    return (limpio
            .dropna(subset=["Fecha", "Ticker"] + precios)
            .filter((F.col("Close") > 0) & (F.col("High") >= F.col("Low")))
            .dropDuplicates(["Ticker", "Fecha"])
            .select("Fecha", "Ticker", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits"))