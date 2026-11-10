"""Gold - tabla de hechos del modelo en estrella: una fila por (Ticker, Fecha).

Usado en: Ej10.
"""

# Usado en: Ej10
def construir_hecho(precios, empresas, sentimiento):
    """Ej10: tabla de hechos. Una fila por (Ticker, Fecha): precios + sentimiento del sector de la empresa.

    El sector se obtiene de la dimensión de empresas y se usa solo para cruzar con el sentimiento;
    no se guarda en el hecho (se alcanza desde dim_empresa). Es un left join: ninguna cotización se
    pierde; si no hay sentimiento para su sector y fecha, Sentimiento queda nulo.
    """
    sectores = empresas.select("Ticker", "Sector")
    return (precios
            .join(sectores, "Ticker", "left")
            .join(sentimiento, ["Fecha", "Sector"], "left")
            .select("Fecha", "Ticker", "Open", "High", "Low", "Close", "Volume",
                    "Dividends", "Stock_Splits", "Sentimiento"))
