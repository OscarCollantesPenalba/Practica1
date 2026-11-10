import time
from datetime import date

import yfinance as yf
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

# Solo Ticker y FechaExtraccion son obligatorios: cualquier dato de Yahoo puede faltar.
ESQUEMA_EMPRESAS = StructType([
    StructField("Ticker", StringType(), False),
    StructField("longName", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("country", StringType(), True),
    StructField("marketCap", LongType(), True),
    StructField("beta", DoubleType(), True),
    StructField("recommendationMean", DoubleType(), True),
    StructField("numberOfAnalystOpinions", LongType(), True),
    StructField("targetMeanPrice", DoubleType(), True),
    StructField("FechaExtraccion", DateType(), False),
])


def _info(ticker):
    """Objeto info de Yahoo. Reintenta 3 veces (Yahoo limita las peticiones); si falla, devuelve {}."""
    # Herramienta IA usada: Claude
    
    for _ in range(3):
        try:
            info = yf.Ticker(ticker).info
            if info:
                return info
        except Exception:
            pass
        time.sleep(2)
    return {}


def _a_tipo(tipo, valor):
    """Convierte el valor al tipo del esquema; si falta o viene raro, devuelve None."""
    # Herramienta IA usada: Claude
    
    try:
        return tipo(valor)
    except (TypeError, ValueError):
        return None


# Usado en: Ej9
def descargar_info_empresas(spark, tickers):
    """Descarga el objeto info de cada ticker y lo devuelve como DataFrame, sin limpiar.
    Un ticker sin datos aparece igualmente, con todo nulo salvo Ticker y FechaExtraccion."""
    # Herramienta IA usada: Claude
    
    filas = []
    for ticker in tickers:
        info = _info(ticker)
        filas.append((ticker, info.get("longName"), info.get("sector"), info.get("industry"),
                      info.get("country"), _a_tipo(int, info.get("marketCap")),
                      _a_tipo(float, info.get("beta")), _a_tipo(float, info.get("recommendationMean")),
                      _a_tipo(int, info.get("numberOfAnalystOpinions")),
                      _a_tipo(float, info.get("targetMeanPrice")), date.today()))
    return spark.createDataFrame(filas, ESQUEMA_EMPRESAS)