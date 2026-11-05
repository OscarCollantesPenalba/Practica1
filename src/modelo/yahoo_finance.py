"""Descarga de precios desde Yahoo Finance (yfinance) hacia DataFrames de Spark."""
import time
from functools import reduce

import pandas as pd
import yfinance as yf
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                               StructField, StructType)

# Ej1a - Esquema de los precios y qué columnas admiten nulos (y por qué).
# Se fija el esquema a mano (en vez de dejar que Spark lo infiera) para que Spark
# rechace una fila con nulo en una columna "no nula" en lugar de guardarla.
# Orden de columnas: el del enunciado. Se descarta "Adj Close" (no está en la lista).
ESQUEMA_PRECIOS = StructType([
    # Date: NO nulo. Junto con Ticker es la clave de la fila (MERGE, anti-join).
    StructField("Date", DateType(), False),
    # Open/High/Low/Close: nulos permitidos. Bronze es la copia fiel de Yahoo y Yahoo
    # puede devolver huecos (sesión sin negociación o dato incompleto); no se inventan
    # valores, la limpieza se hace en silver.
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    # Volume: nulo permitido (lo indica el enunciado: hay sesiones sin dato).
    # LongType y no int: así no hay desbordamiento con valores muy negociados.
    StructField("Volume", LongType(), True),
    # Ticker: NO nulo. Identifica el activo y lo añadimos nosotros, siempre existe.
    StructField("Ticker", StringType(), False),
    # Dividends / Stock_Splits: NO nulos. Yahoo informa 0.0 cuando no hay evento.
    # "Stock Splits" se renombra a Stock_Splits: Parquet/Delta no admiten espacios
    # en los nombres de columna (sin column mapping).
    StructField("Dividends", DoubleType(), False),
    StructField("Stock_Splits", DoubleType(), False),
])


def _a_float(valor):
    return None if pd.isna(valor) else float(valor)


def _a_int(valor):
    return None if pd.isna(valor) else int(valor)


def _descargar_pandas(ticker, start, end, reintentos=3, espera=2):
    """Descarga con yfinance reintentando ante fallos temporales de red o de Yahoo."""
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            pdf = yf.Ticker(ticker).history(start=start, end=end, interval="1d",
                                            auto_adjust=False)
            if pdf is not None and not pdf.empty:
                return pdf
        except Exception as error:  # red, límite de Yahoo, ticker inexistente...
            ultimo_error = error
        if intento < reintentos:
            time.sleep(espera * intento)
    detalle = f": {ultimo_error}" if ultimo_error else ""
    raise ValueError(f"Sin datos de Yahoo Finance para {ticker}{detalle}")


def descargar_historico(spark, ticker, start, end):
    """Ej1a: descarga el histórico diario de un ticker y lo devuelve como DataFrame de Spark."""
    pdf = _descargar_pandas(ticker, start, end)
    # La fecha viene en el índice de pandas; Spark no tiene índices, solo columnas.
    pdf = pdf.reset_index()
    pdf = pdf.rename(columns={pdf.columns[0]: "Date"})
    pdf["Date"] = pd.to_datetime(pdf["Date"]).dt.date  # fecha local de la sesión (sin hora ni zona)

    registros = [
        (f["Date"], _a_float(f["Open"]), _a_float(f["High"]), _a_float(f["Low"]),
         _a_float(f["Close"]), _a_int(f["Volume"]), ticker,
         _a_float(f["Dividends"]), _a_float(f["Stock Splits"]))
        for f in pdf.to_dict("records")
    ]
    return spark.createDataFrame(registros, ESQUEMA_PRECIOS)


def descargar_ibex(spark, tickers, start, end):
    """Ej1b: descarga todos los tickers y los apila con union.

    Devuelve (DataFrame, lista de tickers sin datos).
    """
    dataframes, fallidos = [], []
    for ticker in tickers:
        try:
            dataframes.append(descargar_historico(spark, ticker, start, end))
        except ValueError:
            fallidos.append(ticker)
    if not dataframes:
        raise ValueError("No se pudo descargar ningún ticker (¿sin conexión a internet?).")
    df = reduce(lambda a, b: a.unionByName(b), dataframes)
    # Se materializa una sola vez: la unión de 35 DataFrames creados desde listas de Python es
    # cara de recalcular, y operaciones como MERGE leen la fuente varias veces.
    df.cache()
    df.count()
    return df, fallidos
