from src.modelo import config
from src.modelo.data_lake import contar_filas, escribir_parquet, leer_parquet
from src.modelo.sentimiento import leer_csv_sentimiento
from src.modelo.spark_session import get_spark_session
from src.modelo.yahoo_finance import descargar_historico, descargar_ibex
from src.vista.ibex_view import IbexView

# Ejercicios implementados, en orden de ejecución.
EJERCICIOS = ["ej1a", "ej1b", "ej1c", "ej2a"]


class IbexController:
    """Orquesta los ejercicios: llama a funciones del modelo y se las pasa a la vista.
    La lógica vive en src/modelo; aquí solo se encadenan llamadas y se muestran resultados."""

    def __init__(self):
        self.spark = get_spark_session()
        self.view = IbexView()

    def run(self, ejercicios=None):
        ejercicios = [e.lower() for e in (ejercicios or EJERCICIOS)]
        desconocidos = [e for e in ejercicios if e not in EJERCICIOS]
        if desconocidos:
            print(f"Ejercicio(s) no disponible(s): {desconocidos}. Disponibles: {EJERCICIOS}")
        else:
            for nombre in ejercicios:
                getattr(self, nombre)()
        self.spark.stop()

#========================================================================================
#===================CLASE 1 - Capa bronze ===============================================
#========================================================================================

    def ej1a(self):
        # Ej1a
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1a")
        # Una sola acción (BBVA) para comprobar la función; los nulos están definidos
        # y justificados columna a columna en ESQUEMA_PRECIOS (yahoo_finance.py).
        df = descargar_historico(self.spark, "BBVA.MC", config.INICIO, config.FIN)
        self.view.esquema(df)
        self.view.filas(df, 5)

    def ej1b(self):
        # Ej1b
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1b")
        df, fallidos = descargar_ibex(self.spark, config.TICKERS_IBEX, config.INICIO, config.FIN)
        escribir_parquet(df, config.BRONZE_IBEX, "overwrite")
        self.view.valor("Filas escritas en bronze/ibex", contar_filas(self.spark, config.BRONZE_IBEX))
        if fallidos:
            self.view.valor("Tickers sin datos", fallidos)

    def ej1c(self):
        # Ej1c (opcional)
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1c")
        df = leer_parquet(self.spark, config.BRONZE_IBEX)
        self.view.esquema(df)
        self.view.filas(df, 5)
        # Column pruning: solo se leen 3 de las 9 columnas
        self.view.filas(leer_parquet(self.spark, config.BRONZE_IBEX, ["Ticker", "Date", "Close"]), 5)
        self.view.mensaje("Comentario: Parquet guarda cada columna en su propio bloque (column chunk). "
                          "Al pedir solo Ticker, Date y Close, Spark lee únicamente esos 3 bloques y se "
                          "salta Open, High, Low, Volume, Dividends y Stock_Splits: menos lectura de disco "
                          "y menos memoria. Con un CSV habría que leer todas las filas completas.")

    def ej2a(self):
        # Ej2a
        # Herramienta IA usada: Claude
        self.view.titulo("Ej2a")
        df = leer_csv_sentimiento(self.spark)
        self.view.esquema(df)
        self.view.filas(df, 5)
        self.view.mensaje("Comentario: todas las columnas llegan como string porque un CSV es texto "
                          "plano y no guarda tipos; Spark no los infiere salvo que se le pida "
                          "(inferSchema). Fecha y Sentimiento habrá que convertirlos en silver.")
