from src.modelo import config
from src.modelo.delta_ops import (con_close_mas5, contar_filas_delta, convertir_a_delta,
                                  ficheros_delta_log, historial, intentar_escribir_delta, leer_delta,
                                  merge_insertar_nuevos, version_actual)
from src.modelo.data_lake import contar_filas, escribir_parquet, leer_parquet
from src.modelo.sentimiento import leer_csv_sentimiento
from src.modelo.spark_session import get_spark_session
from src.modelo.yahoo_finance import descargar_historico, descargar_ibex
from src.vista.ibex_view import IbexView

# Ejercicios implementados, en orden de ejecución.
EJERCICIOS = ["ej1a", "ej1b", "ej1c", "ej2a", "ej2b", "ej2c", "ej3", "ej4", "ej5a"]


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

    def ej2b(self):
        # Ej2b
        # Herramienta IA usada: Claude
        self.view.titulo("Ej2b")
        df = leer_csv_sentimiento(self.spark)
        self.view.valor("Filas leídas del CSV", df.count())
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "overwrite")
        self.view.valor("Filas escritas en bronze/sentimiento",
                        contar_filas(self.spark, config.BRONZE_SENTIMIENTO))

    def ej2c(self):
        # Ej2c (opcional)
        # Herramienta IA usada: Claude
        self.view.titulo("Ej2c")
        df = leer_csv_sentimiento(self.spark)
        antes = contar_filas(self.spark, config.BRONZE_SENTIMIENTO)
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "append")
        despues = contar_filas(self.spark, config.BRONZE_SENTIMIENTO)
        self.view.valor("Filas antes del append", antes)
        self.view.valor("Filas después del append", despues)
        self.view.mensaje("Comentario: Parquet es inmutable, así que append solo escribe ficheros nuevos "
                          "sin comprobar nada: cada fila del CSV queda duplicada. No hay clave ni "
                          "transacciones, por eso la carga no es idempotente (Delta lo resuelve con MERGE).")
        # Se deshace el duplicado para que el resto de ejercicios parta de bronze correcta
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "overwrite")
        self.view.valor("Filas tras restaurar con overwrite",
                        contar_filas(self.spark, config.BRONZE_SENTIMIENTO))

#========================================================================================
#===================CLASE 2 - Delta Lake sobre bronze ===================================
#========================================================================================

    def ej3(self):
        # Ej3
        # Herramienta IA usada: Claude
        self.view.titulo("Ej3")
        for nombre, ruta in [("bronze/ibex", config.BRONZE_IBEX),
                             ("bronze/sentimiento", config.BRONZE_SENTIMIENTO)]:
            convertir_a_delta(self.spark, ruta)
            self.view.valor("Tabla Delta", nombre)
            h = historial(self.spark, ruta).select("version", "timestamp", "operation",
                                                   "operationParameters", "operationMetrics")
            self.view.filas(h, 5, False)
            self.view.valor("Ficheros en _delta_log", ficheros_delta_log(ruta))
        self.view.mensaje("Comentario: ha aparecido la carpeta _delta_log con el commit de la versión 0 "
                          "(00000000000000000000.json, operación CONVERT), un checkpoint en Parquet (resumen "
                          "del estado de la tabla para no releer todos los JSON), el puntero _last_checkpoint, "
                          "la carpeta _staged_commits (reservada para commits en preparación) y ficheros .crc "
                          "de checksum de Hadoop. Los ficheros Parquet de datos no se han tocado: solo se han "
                          "registrado en el log. A partir de ahora cada cambio añadirá un JSON nuevo al log, "
                          "y recorrerlo permite reconstruir el estado de la tabla o volver a una versión anterior.")

    def ej4(self):
        # Ej4
        # Herramienta IA usada: Claude
        self.view.titulo("Ej4")
        ruta = config.BRONZE_IBEX
        self.view.valor("Versión actual de bronze/ibex", version_actual(self.spark, ruta))
        self.view.valor("Filas actuales de bronze/ibex", contar_filas_delta(self.spark, ruta))
        # Se vuelve a descargar todo el periodo hasta hoy; MERGE decide qué entra
        nuevos, fallidos = descargar_ibex(self.spark, config.TICKERS_IBEX, config.INICIO, config.hoy())
        if fallidos:
            self.view.valor("Tickers sin datos", fallidos)
        for ejecucion in (1, 2):
            antes = contar_filas_delta(self.spark, ruta)
            merge_insertar_nuevos(self.spark, ruta, nuevos, ["Ticker", "Date"])
            despues = contar_filas_delta(self.spark, ruta)
            self.view.valor(f"MERGE {ejecucion}: filas antes / después / insertadas",
                            f"{antes} / {despues} / {despues - antes}")
            self.view.valor(f"MERGE {ejecucion}: versión de la tabla", version_actual(self.spark, ruta))
        self.view.mensaje("Comentario: MERGE compara cada fila descargada con la tabla por la clave "
                          "(Ticker, Date) e inserta solo las que no existen (whenNotMatchedInsertAll). "
                          "En la primera ejecución entran las sesiones posteriores al 1 de octubre; en la "
                          "segunda todas las filas ya existen y no se inserta ninguna, así que la carga es "
                          "idempotente (con append se duplicarían, como vimos en el Ej2c).")

    def ej5a(self):
        # Ej5a
        # Herramienta IA usada: Claude
        self.view.titulo("Ej5a")
        ruta = config.BRONZE_IBEX
        df_nuevo = con_close_mas5(leer_delta(self.spark, ruta))
        antes = (version_actual(self.spark, ruta), contar_filas_delta(self.spark, ruta))
        aceptada, mensaje = intentar_escribir_delta(df_nuevo, ruta, "append")
        despues = (version_actual(self.spark, ruta), contar_filas_delta(self.spark, ruta))
        self.view.valor("¿Delta acepta la escritura?", aceptada)
        if mensaje:
            self.view.valor("Mensaje de Delta", mensaje)
        self.view.valor("Versión / filas antes", f"{antes[0]} / {antes[1]}")
        self.view.valor("Versión / filas después", f"{despues[0]} / {despues[1]}")
        if aceptada:
            self.view.mensaje("Comentario: Delta ha aceptado la escritura, algo que no se esperaba.")
        else:
            self.view.mensaje("Comentario: Delta valida el esquema al escribir (schema enforcement). El "
                              "DataFrame trae una columna, Close_mas5, que la tabla no tiene, así que "
                              "rechaza el append y la tabla queda intacta (misma versión y mismas filas). "
                              "Protege la tabla de escrituras accidentales; si el cambio es intencionado "
                              "hay que pedirlo expresamente con mergeSchema (Ej5b).")
