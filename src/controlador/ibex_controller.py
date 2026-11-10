from src.modelo.comun import config
from src.modelo.comun.calidad import (claves_con_nulo, contar_nulos, diferencia_filas, integridad_estrella,
                                      perfil_precios, perfil_sentimiento)
from src.modelo.comun.spark_session import get_spark_session
from src.modelo.io.parquet_io import contar_filas, escribir_parquet, leer_parquet
from src.modelo.io.delta_io import contar_filas_delta, escribir_delta, leer_delta, version_actual
from src.modelo.bronze.precios import descargar_historico, descargar_ibex
from src.modelo.bronze.sentimiento import leer_csv_sentimiento
from src.modelo.bronze.empresas import descargar_info_empresas
from src.modelo.bronze.exploracion_api import explorar_yahoo
from src.modelo.bronze.delta_ops import (borrar_filas, con_close_mas5, convertir_a_delta,
                                         historial, intentar_escribir_delta, leer_delta_version,
                                         merge_insertar_nuevos, restaurar_version, version_de_operacion)
from src.modelo.silver.sentimiento import limpiar_sentimiento
from src.modelo.silver.precios import limpiar_precios
from src.modelo.silver.empresas import limpiar_empresas
from src.modelo.gold.hecho import construir_hecho
from src.modelo.gold.dimensiones import construir_dim_empresa, construir_dim_fecha
from src.modelo.gold.indicadores import COLUMNAS_INDICADORES, anadir_indicadores
from src.modelo.gold.cruce import (con_motivo_precios, con_motivo_sentimiento, precios_sin_sentimiento,
                                   sectores_sin_empresas, sentimiento_sin_precios)
from src.modelo.gold.exportacion import exportar_parquet_unico, tamano_kb
from src.vista.ibex_view import IbexView

# Ejercicios implementados, en orden de ejecución.
EJERCICIOS = ["ej1a", "ej1b", "ej1c", "ej2a", "ej2b", "ej2c", "ej3", "ej4", "ej5a", "ej5b", "ej5c", "ej5d", "ej6", "ej7", "ej8", "ej9", "ej10", "ej11", "ej12", "ej13"]
EJERCICIOS = ["ej4"]

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
        self.view.titulo("Ej1a")
        # Una sola acción (BBVA) para comprobar la función; los nulos están definidos 
        # y justificados columna a columna en ESQUEMA_PRECIOS.
        df = descargar_historico(self.spark, "BBVA.MC", config.INICIO, config.FIN)
        self.view.esquema(df)

    def ej1b(self):
        # Ej1b
        self.view.titulo("Ej1b")
        df = descargar_ibex(self.spark, config.TICKERS_IBEX, config.INICIO, config.FIN)
        escribir_parquet(df, config.BRONZE_IBEX, "overwrite")

    def ej1c(self):
        # Ej1c (opcional)   
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
        self.view.titulo("Ej2a")
        df = leer_csv_sentimiento(self.spark)
        self.view.esquema(df)
        self.view.filas(df, 5)
        self.view.mensaje("Comentario: todas las columnas llegan como string porque un CSV es texto "
                          "plano y no guarda tipos.")

    def ej2b(self):
        # Ej2b
        self.view.titulo("Ej2b")
        df = leer_csv_sentimiento(self.spark)
        self.view.valor("Filas leídas del CSV", df.count())
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "overwrite")
        self.view.valor("Filas escritas en bronze/sentimiento",contar_filas(self.spark, config.BRONZE_SENTIMIENTO))

    def ej2c(self):
        # Ej2c (opcional)
        self.view.titulo("Ej2c")
        df = leer_csv_sentimiento(self.spark)
        self.view.valor("Filas antes del append", contar_filas(self.spark, config.BRONZE_SENTIMIENTO))
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "append")
        self.view.valor("Filas después del append", contar_filas(self.spark, config.BRONZE_SENTIMIENTO))
        self.view.mensaje("Comentario: Parquet es inmutable, así que append solo añade ficheros nuevos "
                          "sin comprobar nada: cada fila del CSV queda duplicada. No hay clave ni "
                          "transacciones, por eso la carga no es idempotente (Delta lo resuelve con MERGE).")
        
        # Se deshace el duplicado para que el Ej3 parta de un bronze correcto
        escribir_parquet(df, config.BRONZE_SENTIMIENTO, "overwrite")

#========================================================================================
#===================CLASE 2 - Delta Lake sobre bronze ===================================
#========================================================================================

    def ej3(self):
        # Ej3
        self.view.titulo("Ej3")
        for nombre, ruta in [("bronze/ibex", config.BRONZE_IBEX), ("bronze/sentimiento", config.BRONZE_SENTIMIENTO)]:
            convertir_a_delta(self.spark, ruta)
            self.view.valor("Tabla Delta", nombre)
            h = historial(self.spark, ruta).select("version", "operation", "operationMetrics")
            self.view.filas(h, 5, False)
        self.view.mensaje("Comentario: ha aparecido la carpeta _delta_log con el commit de la versión 0 "
                          "Los ficheros Parquet de datosno se han tocado: solo se han registrado en el "
                          "log. A partir de ahora cada cambio añadirá un JSON nuevo, y recorrerlos permite"
                          "reconstruir el estado de la tabla o volver a una versión anterior.")

    def ej4(self):
        # Ej4
        self.view.titulo("Ej4")
        ruta = config.BRONZE_IBEX
        self.view.valor("Versión actual de bronze/ibex", version_actual(self.spark, ruta))
        self.view.valor("Filas actuales de bronze/ibex", contar_filas_delta(self.spark, ruta))
        # Se vuelve a descargar todo el periodo hasta hoy; MERGE decide qué entra
        nuevos = descargar_ibex(self.spark, config.TICKERS_IBEX, config.INICIO, config.hoy())
        for ejecucion in (1, 2):
            antes = contar_filas_delta(self.spark, ruta)
            merge_insertar_nuevos(self.spark, ruta, nuevos)
            despues = contar_filas_delta(self.spark, ruta)
            self.view.valor(f"MERGE {ejecucion}: filas antes / después / insertadas", f"{antes} / {despues} / {despues - antes}")
            self.view.valor(f"MERGE {ejecucion}: versión de la tabla", version_actual(self.spark, ruta))
            self.view.mensaje("Comentario: MERGE compara cada fila descargada con la tabla por la clave "
                          "(Ticker, Date) e inserta solo las que no existen. La primera vez entran los días "
                          "nuevos; la segunda no hay nada que insertar, así que cargar dos veces no "
                          "duplica datos (con append sí se duplicarían).")

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

    def ej5b(self):
        # Ej5b
        # Herramienta IA usada: Claude
        self.view.titulo("Ej5b")
        ruta = config.BRONZE_IBEX
        df_nuevo = con_close_mas5(leer_delta(self.spark, ruta))
        antes = (version_actual(self.spark, ruta), contar_filas_delta(self.spark, ruta))
        aceptada, mensaje = intentar_escribir_delta(df_nuevo, ruta, "append", True)
        despues = (version_actual(self.spark, ruta), contar_filas_delta(self.spark, ruta))
        self.view.valor("¿Delta acepta la escritura con mergeSchema?", aceptada)
        if mensaje:
            self.view.valor("Mensaje de Delta", mensaje)
        self.view.valor("Versión / filas antes", f"{antes[0]} / {antes[1]}")
        self.view.valor("Versión / filas después", f"{despues[0]} / {despues[1]}")
        tabla = leer_delta(self.spark, ruta)
        self.view.esquema(tabla)
        if aceptada:
            nulos = contar_nulos(tabla, "Close_mas5")
            self.view.valor("Filas con Close_mas5 nulo (las antiguas)", nulos)
            self.view.valor("Filas con Close_mas5 con valor (las nuevas)", despues[1] - nulos)
            self.view.mensaje("Comentario: con mergeSchema=true Delta ya no rechaza la escritura: añade "
                              "Close_mas5 al esquema de la tabla (evolución de esquema) y las filas antiguas, "
                              "que no tienen ese dato, quedan con valor nulo. Como se ha escrito la tabla "
                              "entera, cada fila existe ahora dos veces (una sin Close_mas5 y otra con él); "
                              "el Ej6 lo deshará con RESTORE.")
        else:
            self.view.mensaje("Comentario: Delta sigue rechazando la escritura incluso con mergeSchema.")

    def ej5c(self):
        # Ej5c
        # Herramienta IA usada: Claude
        self.view.titulo("Ej5c")
        ruta = config.BRONZE_IBEX
        condicion = "Ticker = 'BBVA.MC'"
        total_antes = contar_filas_delta(self.spark, ruta)
        version_antes = version_actual(self.spark, ruta)
        self.view.valor("Filas de BBVA.MC antes del DELETE", contar_filas_delta(self.spark, ruta, condicion))
        self.view.filas(leer_delta(self.spark, ruta, condicion), 5)
        borrar_filas(self.spark, ruta, condicion)
        self.view.valor("Filas de BBVA.MC después del DELETE", contar_filas_delta(self.spark, ruta, condicion))
        self.view.filas(leer_delta(self.spark, ruta, condicion), 5)
        self.view.valor("Filas totales antes / después", f"{total_antes} / {contar_filas_delta(self.spark, ruta)}")
        self.view.valor("Versión antes / después", f"{version_antes} / {version_actual(self.spark, ruta)}")
        self.view.mensaje("Comentario: DELETE borra solo las filas que cumplen la condición y deja el resto. "
                          "No elimina los ficheros del disco: crea una versión nueva del log en la que los "
                          "ficheros afectados dejan de formar parte de la tabla (siguen en disco hasta un "
                          "VACUUM), por eso el time travel todavía puede recuperar esas filas.")

    def ej5d(self):
        # Ej5d
        # Herramienta IA usada: Claude
        self.view.titulo("Ej5d")
        ruta = config.BRONZE_IBEX
        h = historial(self.spark, ruta).select("version", "timestamp", "operation",
                                               "operationParameters", "operationMetrics")
        self.view.filas(h, 10, False)
        # La versión de antes del Ej5a es la que dejó el MERGE del Ej4 (el Ej5a no escribió nada)
        version = version_de_operacion(self.spark, ruta, "MERGE")
        if version is None:  # si el MERGE no insertó nada, el estado del Ej4 es el de la conversión
            version = 0
        antigua = leer_delta_version(self.spark, ruta, version)
        actual = leer_delta(self.spark, ruta)
        self.view.valor("Versión de antes del Ej5a (último MERGE)", version)
        self.view.valor(f"versionAsOf {version}: filas / columnas", f"{antigua.count()} / {len(antigua.columns)}")
        self.view.valor("Tabla actual: filas / columnas", f"{actual.count()} / {len(actual.columns)}")
        solo_antigua, solo_actual = diferencia_filas(antigua, actual)
        self.view.valor(f"Filas solo en la versión {version} / solo en la actual", f"{solo_antigua} / {solo_actual}")
        self.view.valor("Versión actual tras leer con versionAsOf", version_actual(self.spark, ruta))
        self.view.mensaje("Comentario: no está todo igual. La versión antigua conserva el estado del Ej4 (sin "
                          "Close_mas5, sin duplicados y con las filas de BBVA), mientras que la tabla actual "
                          "tiene una columna más, filas duplicadas por el Ej5b y no tiene BBVA por el Ej5c. "
                          "versionAsOf solo lee: la tabla actual no cambia (la versión sigue siendo la misma); "
                          "para dejarla como estaba hace falta RESTORE (Ej6).")

    def ej6(self):
        # Ej6
        # Herramienta IA usada: Claude
        self.view.titulo("Ej6")
        ruta = config.BRONZE_IBEX
        # Se vuelve al estado que dejó el MERGE del Ej4 (si no insertó nada, al de la conversión)
        version = version_de_operacion(self.spark, ruta, "MERGE")
        if version is None:
            version = 0
        # Los números se calculan antes de restaurar: el DataFrame es perezoso
        tabla = leer_delta(self.spark, ruta)
        antes = (version_actual(self.spark, ruta), tabla.count(), len(tabla.columns))
        self.view.valor("Antes del RESTORE: versión / filas / columnas", f"{antes[0]} / {antes[1]} / {antes[2]}")
        restaurar_version(self.spark, ruta, version)
        tabla = leer_delta(self.spark, ruta)
        despues = (version_actual(self.spark, ruta), tabla.count(), len(tabla.columns))
        self.view.valor(f"Después del RESTORE a la versión {version}: versión / filas / columnas",
                        f"{despues[0]} / {despues[1]} / {despues[2]}")
        h = historial(self.spark, ruta).select("version", "timestamp", "operation", "operationParameters")
        self.view.filas(h, 5, False)
        objetivo = leer_delta_version(self.spark, ruta, version)
        solo_objetivo, solo_tabla = diferencia_filas(objetivo, tabla)
        mismas_columnas = objetivo.columns == tabla.columns
        self.view.valor(f"Filas solo en la versión {version} / solo en la tabla actual", f"{solo_objetivo} / {solo_tabla}")
        self.view.valor("¿Mismas columnas?", mismas_columnas)
        self.view.mensaje("Comentario: a diferencia de versionAsOf, que solo lee, RESTORE cambia la tabla "
                          "actual. No borra el historial: crea una versión nueva cuyo contenido es idéntico "
                          "al de la versión indicada, así que el log conserva todo lo ocurrido, incluido el "
                          "propio RESTORE.")
        if solo_objetivo == 0 and solo_tabla == 0 and mismas_columnas:
            self.view.mensaje(f"Comprobación: la tabla actual es idéntica a la versión {version}.")
        else:
            self.view.mensaje(f"Aviso: la tabla actual no coincide del todo con la versión {version}.")

#========================================================================================
#===================CLASE 3 - Capas silver y gold =======================================
#========================================================================================

    def ej7(self):
        # Ej7
        # Herramienta IA usada: Claude
        self.view.titulo("Ej7")
        self.view.seccion("Perfil de bronze/ibex (precios)")
        for etiqueta, valor in perfil_precios(leer_delta(self.spark, config.BRONZE_IBEX)).items():
            self.view.valor(etiqueta, valor)
        self.view.seccion("Perfil de bronze/sentimiento (CSV del experto)")
        for etiqueta, valor in perfil_sentimiento(leer_delta(self.spark, config.BRONZE_SENTIMIENTO)).items():
            self.view.valor(etiqueta, valor)
        self.view.seccion("Datos que ofrece Yahoo Finance (BBVA.MC)")
        n_campos, seleccion, fuentes = explorar_yahoo("BBVA.MC")
        self.view.valor("Campos en el objeto info", n_campos)
        for campo, valor in seleccion.items():
            self.view.valor(f"  info.{campo}", valor)
        for fuente, detalle in fuentes:
            self.view.valor(f"  {fuente}", detalle)
        self.view.mensaje("Comentario: la reflexión sobre cómo incorporar la información del experto con "
                          "estos datos está en el PDF.")

    def ej8(self):
        # Ej8
        # Herramienta IA usada: Claude
        self.view.titulo("Ej8")
        bronce = leer_delta(self.spark, config.BRONZE_SENTIMIENTO)
        escribir_delta(limpiar_sentimiento(bronce), config.SILVER_SENTIMIENTO, "overwrite", overwrite_schema=True)
        plata = leer_delta(self.spark, config.SILVER_SENTIMIENTO)
        self.view.esquema(plata)
        self.view.filas(plata.orderBy("Fecha", "Sector"), 5)
        filas_bronce, filas_plata = contar_filas_delta(self.spark, config.BRONZE_SENTIMIENTO), plata.count()
        self.view.valor("Filas de bronze/sentimiento", filas_bronce)
        self.view.valor("Filas de silver/sentimiento", filas_plata)
        self.view.valor("Filas descartadas", filas_bronce - filas_plata)
        self.view.valor("Sectores en silver", [r["Sector"] for r in plata.select("Sector").distinct().orderBy("Sector").collect()])
        self.view.mensaje("Comentario: Fecha pasa de texto a date y Sentimiento de texto a double (con try_cast, "
                          "aceptando coma decimal). Sector se normaliza (sin espacios y con el formato de "
                          "Yahoo, p. ej. Financial Services) para poder cruzarlo con el sector de cada empresa. "
                          "Se descartan los nulos y los duplicados por (Fecha, Sector).")

    def ej9(self):
        # Ej9
        # Herramienta IA usada: Claude
        self.view.titulo("Ej9")
        # Dimensión de empresas: crudo (bronze) y corregido (silver)
        info, sin_info = descargar_info_empresas(self.spark, config.TICKERS_IBEX)
        escribir_delta(info, config.BRONZE_EMPRESAS, "overwrite", overwrite_schema=True)
        empresas_bronce = leer_delta(self.spark, config.BRONZE_EMPRESAS)
        escribir_delta(limpiar_empresas(empresas_bronce), config.SILVER_EMPRESAS, "overwrite", overwrite_schema=True)
        empresas = leer_delta(self.spark, config.SILVER_EMPRESAS)
        # Precios: silver
        precios_bronce = leer_delta(self.spark, config.BRONZE_IBEX)
        escribir_delta(limpiar_precios(precios_bronce), config.SILVER_IBEX, "overwrite", overwrite_schema=True)
        precios = leer_delta(self.spark, config.SILVER_IBEX)
        filas_bronce, filas_plata = precios_bronce.count(), precios.count()
        self.view.valor("Filas de bronze/ibex / silver/ibex / descartadas",
                        f"{filas_bronce} / {filas_plata} / {filas_bronce - filas_plata}")
        self.view.valor("Filas de bronze/empresas / silver/empresas",
                        f"{empresas_bronce.count()} / {empresas.count()}")
        if sin_info:
            self.view.valor("Tickers sin datos de Yahoo", sin_info)
        self.view.valor("Tickers sin sector en silver/empresas", claves_con_nulo(empresas, "Ticker", "Sector"))
        self.view.esquema(precios)
        self.view.filas(precios.orderBy("Ticker", "Fecha"), 5)
        self.view.filas(empresas.select("Ticker", "Nombre", "Sector", "Sector_ES", "Industria", "Pais").orderBy("Ticker"), 5)
        self.view.mensaje("Comentario: en silver/ibex Date pasa a Fecha, los precios y dividendos se redondean a "
                          "4 decimales (se elimina el ruido de float32) y se descartan las sesiones incompletas "
                          "o incoherentes y los duplicados. El sector de cada empresa no viene en el histórico: "
                          "se descarga del objeto info de Yahoo y se guarda como dimensión aparte, en crudo "
                          "(bronze/empresas) y corregida (silver/empresas, con el sector también en español).")

    def ej10(self):
        # Ej10
        # Herramienta IA usada: Claude
        self.view.titulo("Ej10")
        precios = leer_delta(self.spark, config.SILVER_IBEX)
        empresas = leer_delta(self.spark, config.SILVER_EMPRESAS)
        sentimiento = leer_delta(self.spark, config.SILVER_SENTIMIENTO)
        hecho = construir_hecho(precios, empresas, sentimiento)
        escribir_delta(hecho, config.GOLD_HECHO, "overwrite", overwrite_schema=True)
        hecho = leer_delta(self.spark, config.GOLD_HECHO)
        escribir_delta(construir_dim_empresa(empresas), config.GOLD_DIM_EMPRESA, "overwrite", overwrite_schema=True)
        escribir_delta(construir_dim_fecha(hecho), config.GOLD_DIM_FECHA, "overwrite", overwrite_schema=True)
        filas_hecho = hecho.count()
        con_sentimiento = hecho.filter("Sentimiento is not null").count()
        self.view.valor("Filas de silver/ibex / hecho_cotizacion", f"{precios.count()} / {filas_hecho}")
        self.view.valor("Filas del hecho con sentimiento / sin sentimiento",
                        f"{con_sentimiento} / {filas_hecho - con_sentimiento}")
        self.view.valor("Filas de dim_empresa", contar_filas_delta(self.spark, config.GOLD_DIM_EMPRESA))
        self.view.valor("Filas de dim_fecha", contar_filas_delta(self.spark, config.GOLD_DIM_FECHA))
        self.view.esquema(hecho)
        self.view.filas(hecho.orderBy("Ticker", "Fecha"), 5)
        self.view.mensaje("Comentario: modelo en estrella. El hecho (hecho_cotizacion) tiene una fila por "
                          "empresa y día con las medidas (precios, volumen, sentimiento); las dimensiones "
                          "dim_empresa (Ticker, sector, industria...) y dim_fecha (año, mes, día de la "
                          "semana...) aportan los atributos para filtrar y agrupar. El cruce es un left join: "
                          "no se pierde ninguna cotización, y si no hay sentimiento queda nulo.")

    def ej11(self):
        # Ej11 (opcional)
        # Herramienta IA usada: Claude
        self.view.titulo("Ej11")
        precios = leer_delta(self.spark, config.SILVER_IBEX)
        empresas = leer_delta(self.spark, config.SILVER_EMPRESAS)
        sentimiento = leer_delta(self.spark, config.SILVER_SENTIMIENTO)
        # (a) precios que se quedan sin sentimiento
        a = con_motivo_precios(precios_sin_sentimiento(precios, empresas, sentimiento), sentimiento)
        self.view.valor("(a) Cotizaciones sin sentimiento", a.count())
        self.view.filas(a.orderBy("Fecha", "Ticker"), 5)
        self.view.filas(a.groupBy("Motivo").count(), 10, False)
        # (b) sentimiento que se queda sin precios
        b = con_motivo_sentimiento(sentimiento_sin_precios(precios, empresas, sentimiento), precios, empresas)
        self.view.valor("(b) Filas de sentimiento sin cotizaciones", b.count())
        self.view.filas(b.orderBy("Fecha", "Sector"), 5)
        self.view.filas(b.groupBy("Motivo").count(), 10, False)
        self.view.valor("Sectores con sentimiento y sin empresas en el IBEX", sectores_sin_empresas(sentimiento, empresas))
        self.view.mensaje("Comentario: el anti-join muestra lo que no cruza. (a) Las cotizaciones sin sentimiento son "
                          "sobre todo las posteriores al último dato del experto: el CSV termina el 30/09/2026 y los "
                          "precios llegan hasta hoy. (b) El sentimiento sin precios es el anterior al primer precio "
                          "(el CSV empieza en 2021 y los precios en octubre de 2024) y el de Crypto (no hay empresas "
                          "del IBEX en ese sector); si hubiera días sin sesión (festivos) saldrían en su propio "
                          "grupo. Solución: no se pierde nada; el "
                          "left join del Ej10 conserva las cotizaciones sin sentimiento (queda nulo y el dashboard "
                          "puede filtrarlas) y el sentimiento sin precios sigue en silver/sentimiento, fuera del hecho "
                          "porque este tiene granularidad empresa-día. No se rellenan los huecos arrastrando el último "
                          "valor, porque sería inventar la opinión del experto: lo correcto es pedir el CSV actualizado.")

    def ej12(self):
        # Ej12 (opcional)
        # Herramienta IA usada: Claude
        self.view.titulo("Ej12")
        version_antes = version_actual(self.spark, config.GOLD_HECHO)
        # Se materializa antes de escribir: se lee y se reescribe la misma tabla
        con_indicadores = anadir_indicadores(leer_delta(self.spark, config.GOLD_HECHO)).cache()
        con_indicadores.count()
        escribir_delta(con_indicadores, config.GOLD_HECHO, "overwrite", overwrite_schema=True)
        hecho = leer_delta(self.spark, config.GOLD_HECHO)
        self.view.valor("Versión de hecho_cotizacion antes / después",
                        f"{version_antes} / {version_actual(self.spark, config.GOLD_HECHO)}")
        self.view.esquema(hecho)
        self.view.valor("Nulos por columna calculada", {c: contar_nulos(hecho, c) for c in COLUMNAS_INDICADORES})
        self.view.valor("Filas con Tiene_Sentimiento = false", hecho.filter("NOT Tiene_Sentimiento").count())
        # Zona donde acaba el CSV del experto: se ve cómo cambian Sentimiento y Tiene_Sentimiento
        muestra = (hecho.filter("Ticker = 'BBVA.MC' AND Fecha BETWEEN '2026-09-28' AND '2026-10-02'")
                   .orderBy("Fecha")
                   .select("Fecha", "Close", "Variacion_Diaria_Pct", "Rango_Intradia_Pct", "Media_Movil_20",
                           "Sentimiento", "Sentimiento_Anterior", "Tiene_Sentimiento"))
        self.view.filas(muestra, 5)
        self.view.mensaje("Comentario: se añaden al hecho cinco columnas calculadas por empresa y en orden de "
                          "fecha: variación diaria del cierre (%), rango intradía (% del cierre), media móvil de "
                          "20 sesiones, sentimiento de la sesión anterior y un indicador de si hay sentimiento. "
                          "Así Power BI las usa directamente en lugar de recalcularlas, y quedan listas para "
                          "contrastar el sentimiento del experto con el retorno (reflexión del Ej7). Se guardan "
                          "reescribiendo la tabla con una versión nueva de Delta, así que el historial se conserva.")

    def ej13(self):
        # Ej13
        # Herramienta IA usada: Claude
        self.view.titulo("Ej13")
        hecho = leer_delta(self.spark, config.GOLD_HECHO)
        dim_empresa = leer_delta(self.spark, config.GOLD_DIM_EMPRESA)
        dim_fecha = leer_delta(self.spark, config.GOLD_DIM_FECHA)
        # Antes de exportar se comprueba que la estrella es consistente (todo debe ser 0)
        for etiqueta, valor in integridad_estrella(hecho, dim_empresa, dim_fecha).items():
            self.view.valor(etiqueta, valor)
        tablas = [("hecho_cotizacion", hecho, ["Ticker", "Fecha"]),
                  ("dim_empresa", dim_empresa, ["Ticker"]),
                  ("dim_fecha", dim_fecha, ["Fecha"])]
        for nombre, df, orden in tablas:
            fichero = config.EXPORT_GOLD_DIR / f"{nombre}.parquet"
            exportar_parquet_unico(df, fichero, orden)
            exportado = self.spark.read.parquet(fichero.as_posix())
            self.view.valor(f"{nombre}: filas en gold / exportadas / tamaño",
                            f"{df.count()} / {exportado.count()} / {tamano_kb(fichero)} KB")
        self.view.esquema(self.spark.read.parquet((config.EXPORT_GOLD_DIR / "hecho_cotizacion.parquet").as_posix()))
        self.view.mensaje("Comentario: se exportan las tres tablas de la estrella, cada una como un único fichero "
                          "Parquet limpio en data/export/gold: hecho_cotizacion.parquet, dim_empresa.parquet y "
                          "dim_fecha.parquet. Van ordenadas por su clave, sin particionar (son tablas pequeñas) y con "
                          "Snappy, y solo usan tipos simples (date, double, long, boolean y string) que Power BI "
                          "lee sin conversiones. Las claves son Ticker y Fecha (tipo date, necesaria para la tabla de "
                          "fechas de Power BI) y antes de exportar se ha comprobado que no hay claves huérfanas ni "
                          "repetidas.")
