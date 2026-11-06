"""Configuración central: rutas del data lake, periodo de descarga y tickers."""
import os
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"

# El lago se puede mover con la variable de entorno LAKE_DIR (útil si la ruta del
# proyecto tiene espacios o tildes y Hadoop/Delta dan problemas).
LAKE_DIR = Path(os.environ.get("LAKE_DIR", DATA_DIR / "lake"))
EXPORT_DIR = DATA_DIR / "export"

CSV_SENTIMIENTO = DATA_DIR / "sentimiento_ibex.csv"

# Rutas con barras "/" (Spark las acepta también en Windows).
BRONZE_IBEX = (LAKE_DIR / "bronze" / "ibex").as_posix()
BRONZE_SENTIMIENTO = (LAKE_DIR / "bronze" / "sentimiento").as_posix()
BRONZE_EMPRESAS = (LAKE_DIR / "bronze" / "empresas").as_posix()
SILVER_SENTIMIENTO = (LAKE_DIR / "silver" / "sentimiento").as_posix()
SILVER_IBEX = (LAKE_DIR / "silver" / "ibex").as_posix()
SILVER_EMPRESAS = (LAKE_DIR / "silver" / "empresas").as_posix()

# Periodo: últimos 2 años, con último día el 1 de octubre de 2026.
# En yfinance el parámetro end es EXCLUSIVO, por eso se pone el día siguiente.
INICIO = "2024-10-01"
FIN = "2026-10-02"

# IBEX-35. Composición estable desde el 9/07/2024 (Puig sustituyó a Meliá) hasta,
# al menos, junio de 2026, por lo que cubre todo el periodo.
TICKERS_IBEX = [
    "IBE.MC", "REP.MC", "NTGY.MC", "ELE.MC", "ENG.MC", "RED.MC", "SAN.MC",
    "BBVA.MC", "CABK.MC", "BKT.MC", "SAB.MC", "UNI.MC", "MAP.MC", "ACS.MC",
    "ANA.MC", "ANE.MC", "ACX.MC", "MTS.MC", "SCYR.MC", "CLNX.MC", "TEF.MC",
    "AENA.MC", "FER.MC", "ITX.MC", "AMS.MC", "IAG.MC", "GRF.MC", "FDR.MC",
    "SLR.MC", "ROVI.MC", "LOG.MC", "IDR.MC", "PUIG.MC", "COL.MC", "MRL.MC",
]


def hoy():
    """Fecha de hoy (YYYY-MM-DD). Usada como final EXCLUSIVO de la descarga: así se llega
    hasta la última sesión cerrada y no se guarda la sesión de hoy a medias."""
    return date.today().isoformat()


# Sector (nombre de Yahoo, que es también el del CSV de sentimiento) -> nombre en español para el dashboard
SECTORES_ES = {
    "Basic Materials": "Materiales básicos",
    "Communication Services": "Servicios de comunicación",
    "Consumer Cyclical": "Consumo cíclico",
    "Consumer Defensive": "Consumo defensivo",
    "Crypto": "Criptomonedas",
    "Energy": "Energía",
    "Financial Services": "Servicios financieros",
    "Healthcare": "Salud",
    "Industrials": "Industriales",
    "Real Estate": "Inmobiliario",
    "Technology": "Tecnología",
    "Utilities": "Servicios públicos",
}
