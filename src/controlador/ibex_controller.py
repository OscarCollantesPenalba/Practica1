from src.modelo import config
from src.modelo.spark_session import get_spark_session
from src.modelo.yahoo_finance import descargar_historico
from src.vista.ibex_view import IbexView

# Ejercicios implementados, en orden de ejecución.
EJERCICIOS = ["ej1a"]


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
