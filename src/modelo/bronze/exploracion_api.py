import yfinance as yf

# Campos de interés del objeto info de Yahoo (descriptivos, fundamentales y de analistas)
CAMPOS_INFO = [
    "longName", "sector", "industry", "country", "currency", "marketCap", "beta",
    "trailingPE", "forwardPE", "dividendYield", "payoutRatio", "averageVolume",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "recommendationKey", "recommendationMean",
    "numberOfAnalystOpinions", "targetMeanPrice", "targetHighPrice", "targetLowPrice",
]


# Usado en: Ej7
def info_yahoo(ticker):
    """Objeto info de un ticker: el total de campos que trae y el valor de los de interés."""
    try:
        info = yf.Ticker(ticker).info
    except Exception:
        info = {}
    resultado = {"Total de campos en info": len(info)}
    for campo in CAMPOS_INFO:
        resultado[campo] = info.get(campo)
    return resultado


# Usado en: Ej7
def tablas_yahoo(ticker, indice="^IBEX"):
    """Otras fuentes de Yahoo: {nombre: descripción} (filas y columnas, o claves si es un diccionario).
    Si una fuente falla se indica y se sigue con la siguiente."""
    t = yf.Ticker(ticker)
    fuentes = {
        "recommendations (votos de analistas por mes)": lambda: t.recommendations,
        "upgrades_downgrades (cambios de recomendación)": lambda: t.upgrades_downgrades,
        "analyst_price_targets (precio objetivo)": lambda: t.analyst_price_targets,
        "calendar (próximos resultados y dividendos)": lambda: t.calendar,
        f"índice {indice} (histórico, 1 mes)": lambda: yf.Ticker(indice).history(period="1mo"),
    }
    resultado = {}
    for nombre, obtener in fuentes.items():
        try:
            dato = obtener()
            if isinstance(dato, dict):
                resultado[nombre] = f"{len(dato)} claves: {list(dato)}"
            else:
                resultado[nombre] = f"{len(dato)} filas, columnas: {list(dato.columns)}"
        except Exception:
            resultado[nombre] = "no disponible"
    return resultado