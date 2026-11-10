# Usado en: Ej10
def construir_hecho(precios, empresas, sentimiento):
    """Ej10: tabla de hechos. Una fila por (Ticker, Fecha): precios + sentimiento del sector de la empresa.
    """
    sectores = empresas.select("Ticker", "Sector")
    return (precios
            .join(sectores, "Ticker", "left")
            .join(sentimiento, ["Fecha", "Sector"], "left")
            .select("Fecha", "Ticker", "Open", "High", "Low", "Close", "Volume",
                    "Dividends", "Stock_Splits", "Sentimiento"))
