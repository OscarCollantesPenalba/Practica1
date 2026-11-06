"""Limpieza de bronze hacia silver: tipos correctos, valores normalizados y sin duplicados.

Spark 4 trabaja con el modo ANSI activado: un cast o to_date sobre un valor mal formado lanza
error. Por eso se usa try_cast, que devuelve nulo y permite descartar esas filas de forma controlada.
"""
from pyspark.sql import functions as F


def limpiar_sentimiento(df):
    """Ej8: sentimiento crudo (todo texto) -> Fecha (date), Sector (normalizado), Sentimiento (double).

    - Fecha: texto -> date.
    - Sector: sin espacios sobrantes y con el formato de Yahoo ("Financial Services"), para poder
      cruzarlo con el sector de cada empresa.
    - Sentimiento: texto -> double, aceptando la coma como separador decimal.
    - Se descartan las filas con algún campo nulo y los duplicados por (Fecha, Sector).
    """
    fecha = F.expr("try_cast(trim(Fecha) as date)")
    sector = F.initcap(F.lower(F.trim(F.col("Sector"))))
    sentimiento = F.expr("try_cast(regexp_replace(trim(Sentimiento), ',', '.') as double)")
    limpio = df.select(fecha.alias("Fecha"), sector.alias("Sector"), sentimiento.alias("Sentimiento"))
    return (limpio
            .dropna(subset=["Fecha", "Sector", "Sentimiento"])
            .filter(F.col("Sector") != "")
            .dropDuplicates(["Fecha", "Sector"]))
