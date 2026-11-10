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


# Usado en: Ej1a, Ej1b, Ej4
def descargar_historico(spark, ticker, start, end):
    """Ej1a: descarga el histórico diario de un ticker y lo devuelve como DataFrame de Spark."""
    pdf = yf.Ticker(ticker).history(start=start, end=end, interval="1d", auto_adjust=False)
    if pdf.empty:
        raise ValueError(f"Sin datos de Yahoo Finance para {ticker}")
    pdf.reset_index(inplace=True)           # el índice Date pasa a ser una columna
    pdf["Date"] = pdf["Date"].dt.date       # de fecha con hora y zona a solo fecha
    pdf["Ticker"] = ticker
    pdf = pdf.rename(columns={"Stock Splits": "Stock_Splits"})
    pdf = pdf[["Date", "Open", "High", "Low", "Close", "Volume", "Ticker", "Dividends", "Stock_Splits"]]
    return spark.createDataFrame(pdf, schema=ESQUEMA_PRECIOS) # espero: Date: timestamp

# Usado en: Ej1b, Ej4
def descargar_ibex(spark, tickers, start, end):
    """Ej1b: descarga todos los tickers y los apila con union."""
    df = descargar_historico(spark, tickers[0], start, end)
    for ticker in tickers[1:]:
        df = df.union(descargar_historico(spark, ticker, start, end))
    return df
