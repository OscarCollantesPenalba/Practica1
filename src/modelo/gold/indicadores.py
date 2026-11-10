"""Ej12: columnas calculadas del hecho para el dashboard.

Se calculan por empresa (ventana por Ticker) y en orden de fecha, a partir de columnas base del hecho,
por lo que se pueden recalcular las veces que haga falta sin cambiar el resultado.

Usado en: Ej12.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F


# Usado en: Ej12
def anadir_indicadores(hecho):
    """Añade cuatro columnas al hecho:
    - Variacion_Diaria_Pct: variación del cierre respecto a la sesión anterior, en %. Nula en la
      primera sesión de cada empresa (no hay día anterior).
    - Rango_Intradia_Pct: (máximo - mínimo) del día como % del cierre; mide la volatilidad del día.
    - Variacion_Acumulada_Pct: variación del cierre respecto a la primera sesión de la empresa, en %.
    - Media_Movil_20: media del cierre de las últimas 20 sesiones. Nula hasta tener 20 sesiones
      completas, para no mostrar medias calculadas con menos datos.
    """
     # Herramienta IA usada: Claude
    
    ventana = Window.partitionBy("Ticker").orderBy("Fecha")
    ultimas_20 = ventana.rowsBetween(-19, 0)
    cierre_anterior = F.lag("Close").over(ventana)
    primer_cierre = F.first("Close").over(ventana)
    return (hecho
            .withColumn("Variacion_Diaria_Pct", F.round((F.col("Close") - cierre_anterior) / cierre_anterior * 100, 2))
            .withColumn("Rango_Intradia_Pct", F.round((F.col("High") - F.col("Low")) / F.col("Close") * 100, 2))
            .withColumn("Variacion_Acumulada_Pct", F.round((F.col("Close") - primer_cierre) / primer_cierre * 100, 2))
            .withColumn("Media_Movil_20",
                        F.when(F.count("Close").over(ultimas_20) == 20, F.round(F.avg("Close").over(ultimas_20), 2))))