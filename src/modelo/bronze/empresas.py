"""Bronze - empresas: objeto info de Yahoo de cada ticker, tal cual y sin limpiar (bronze/empresas).

Usado en: Ej9.
"""
import time
from datetime import date

import yfinance as yf
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

# Ej9 - Esquema de bronze/empresas: valores del objeto info de Yahoo tal cual, sin limpiar.
# Solo Ticker y FechaExtraccion son obligatorios: cualquier dato de Yahoo puede faltar.
ESQUEMA_EMPRESAS = StructType([
    StructField("Ticker", StringType(), False),
    StructField("longName", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("marketCap", LongType(), True),
    StructField("beta", DoubleType(), True),
    StructField("recommendationKey", StringType(), True),
    StructField("recommendationMean", DoubleType(), True),
    StructField("numberOfAnalystOpinions", LongType(), True),
    StructField("targetMeanPrice", DoubleType(), True),
    StructField("FechaExtraccion", DateType(), False),
])


def _convertir(funcion, valor):
    """Aplica la conversión de tipo; si Yahoo trae algo inesperado, devuelve None en vez de romper."""
    try:
        return funcion(valor)
    except (TypeError, ValueError):
        return None


def _info_con_reintentos(ticker, reintentos=3, espera=2):
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            info = yf.Ticker(ticker).info
            if info:
                return info
        except Exception as error:
            ultimo_error = error
        if intento < reintentos:
            time.sleep(espera * intento)
    raise ValueError(f"Sin info de Yahoo Finance para {ticker}" + (f": {ultimo_error}" if ultimo_error else ""))


# Usado en: Ej9
def descargar_info_empresas(spark, tickers):
    """Ej9: descarga el objeto info de cada ticker. Devuelve (DataFrame, tickers sin datos).

    Los tickers sin datos aparecen igualmente, con todo nulo salvo Ticker y FechaExtraccion,
    para que la dimensión tenga siempre una fila por empresa.
    """
    hoy, filas, fallidos = date.today(), [], []
    for ticker in tickers:
        try:
            info = _info_con_reintentos(ticker)
        except ValueError:
            info, fallidos = {}, fallidos + [ticker]
        texto = lambda c: _convertir(str, info.get(c)) if info.get(c) is not None else None
        filas.append((ticker, texto("longName"), texto("sector"), texto("industry"), texto("country"),
                      texto("currency"), texto("exchange"), _convertir(int, info.get("marketCap")),
                      _convertir(float, info.get("beta")), texto("recommendationKey"),
                      _convertir(float, info.get("recommendationMean")),
                      _convertir(int, info.get("numberOfAnalystOpinions")),
                      _convertir(float, info.get("targetMeanPrice")), hoy))
    if len(fallidos) == len(tickers):
        raise ValueError("No se pudo obtener la info de ningún ticker (¿sin conexión o límite de Yahoo?).")
    return spark.createDataFrame(filas, ESQUEMA_EMPRESAS), fallidos
