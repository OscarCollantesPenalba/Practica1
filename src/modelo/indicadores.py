"""Ej12: columnas calculadas del hecho para el dashboard (medidas derivadas).

Todas se calculan por empresa (partición por Ticker) y en orden de fecha, a partir de columnas
base del hecho; por eso se pueden recalcular las veces que haga falta sin cambiar el resultado.
Con el modo ANSI de Spark 4 una división por cero daría error: se usa try_divide, que devuelve nulo.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

COLUMNAS_INDICADORES = ["Variacion_Diaria_Pct", "Rango_Intradia_Pct", "Media_Movil_20",
                        "Sentimiento_Anterior", "Tiene_Sentimiento"]


def anadir_indicadores(hecho):
    """Añade al hecho cinco columnas calculadas.

    - Variacion_Diaria_Pct: variación del cierre respecto a la sesión anterior, en %. Nula en la
      primera sesión de cada empresa (no hay día anterior).
    - Rango_Intradia_Pct: (máximo - mínimo) del día como % del cierre; mide la volatilidad del día.
    - Media_Movil_20: media del cierre de las últimas 20 sesiones. Nula hasta tener 20 sesiones
      completas, para no mostrar medias calculadas con menos datos.
    - Sentimiento_Anterior: sentimiento de la sesión anterior. Permite estudiar si el sentimiento
      de ayer se relaciona con el retorno de hoy (reflexión del Ej7).
    - Tiene_Sentimiento: indica si la fila tiene sentimiento, para filtrar fácilmente los días que
      el CSV del experto no cubre (Ej11).
    """
    ventana = Window.partitionBy("Ticker").orderBy("Fecha")
    ultimas_20 = ventana.rowsBetween(-19, 0)
    return (hecho
            .withColumn("_cierre_anterior", F.lag("Close").over(ventana))
            .withColumn("Variacion_Diaria_Pct",
                        F.round(F.expr("try_divide(Close - _cierre_anterior, _cierre_anterior) * 100"), 4))
            .withColumn("Rango_Intradia_Pct", F.round(F.expr("try_divide(High - Low, Close) * 100"), 4))
            .withColumn("Media_Movil_20",
                        F.when(F.count("Close").over(ultimas_20) == 20, F.round(F.avg("Close").over(ultimas_20), 4)))
            .withColumn("Sentimiento_Anterior", F.lag("Sentimiento").over(ventana))
            .withColumn("Tiene_Sentimiento", F.col("Sentimiento").isNotNull())
            .drop("_cierre_anterior"))
