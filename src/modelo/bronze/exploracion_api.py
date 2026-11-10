"""Bronze - exploración de la API de Yahoo: qué datos ofrece además del histórico de precios.

Usado en: Ej7.
"""
import yfinance as yf

# Ej7 - Campos de interés del objeto info de Yahoo (descriptivos, fundamentales y de analistas)
CAMPOS_INFO = [
    "longName", "sector", "industry", "country", "currency", "marketCap", "beta",
    "trailingPE", "forwardPE", "dividendYield", "payoutRatio", "averageVolume",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "recommendationKey", "recommendationMean",
    "numberOfAnalystOpinions", "targetMeanPrice", "targetHighPrice", "targetLowPrice",
]


def _resumir(valor):
    """Texto corto que describe lo que ha devuelto Yahoo (tabla, diccionario u otro)."""
    if hasattr(valor, "shape"):
        columnas = list(getattr(valor, "columns", []))[:8]
        return f"{valor.shape[0]} filas" + (f", columnas: {columnas}" if columnas else "")
    if isinstance(valor, dict):
        return f"{len(valor)} claves: {list(valor)[:8]}"
    return str(valor)[:120]


# Usado en: Ej7
def explorar_yahoo(ticker, indice="^IBEX"):
    """Ej7: qué datos ofrece Yahoo además del histórico de precios.

    Devuelve (nº de campos de info, {campo: valor} de los campos de interés,
    [(fuente, descripción o motivo de no disponible)]). Nunca lanza: cada fuente falla por separado.
    """
    t = yf.Ticker(ticker)
    try:
        info = t.info or {}
    except Exception:
        info = {}
    seleccion = {campo: info.get(campo) for campo in CAMPOS_INFO}
    fuentes = {
        "recommendations (votos de analistas por mes)": lambda: t.recommendations,
        "upgrades_downgrades (cambios de recomendación)": lambda: t.upgrades_downgrades,
        "analyst_price_targets (precio objetivo)": lambda: t.analyst_price_targets,
        "calendar (próximos resultados y dividendos)": lambda: t.calendar,
        f"índice {indice} (histórico, 1 mes)": lambda: yf.Ticker(indice).history(period="1mo", interval="1d"),
    }
    resultado = []
    for nombre, obtener in fuentes.items():
        try:
            resultado.append((nombre, _resumir(obtener())))
        except Exception as error:
            lineas = str(error).splitlines()
            resultado.append((nombre, "no disponible: " + (lineas[0][:100] if lineas else type(error).__name__)))
    return len(info), seleccion, resultado
