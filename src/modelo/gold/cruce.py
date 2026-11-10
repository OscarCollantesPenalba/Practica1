"""Gold - diagnóstico del cruce precios/sentimiento con anti-joins.

Usado en: Ej11.
"""
from pyspark.sql import functions as F


# Usado en: Ej11
def precios_sin_sentimiento(precios, empresas, sentimiento):
    """Ej11 (a): cotizaciones cuyo (Fecha, Sector) no tiene sentimiento. Anti-join: filas de la
    izquierda (precios con el sector de su empresa) sin pareja en la derecha (sentimiento)."""
    con_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker", "left")
    return con_sector.join(sentimiento.select("Fecha", "Sector"), ["Fecha", "Sector"], "left_anti")


# Usado en: Ej11
def sentimiento_sin_precios(precios, empresas, sentimiento):
    """Ej11 (b): filas de sentimiento cuyo (Fecha, Sector) no tiene ninguna cotización."""
    fechas_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker").select("Fecha", "Sector").distinct()
    return sentimiento.join(fechas_sector, ["Fecha", "Sector"], "left_anti")


# Usado en: Ej11
def con_motivo_precios(sin_sentimiento, sentimiento):
    """Añade la columna Motivo a las cotizaciones sin sentimiento (a)."""
    ultima = sentimiento.agg(F.max("Fecha")).collect()[0][0]
    return sin_sentimiento.withColumn(
        "Motivo",
        F.when(F.col("Sector").isNull(), "Empresa sin sector")
         .when(F.col("Fecha") > F.lit(ultima), "Posterior al último dato del experto")
         .otherwise("Sin dato del experto ese día"))


# Usado en: Ej11
def con_motivo_sentimiento(sin_precios, precios, empresas):
    """Añade la columna Motivo al sentimiento sin precios (b)."""
    primera = precios.agg(F.min("Fecha")).collect()[0][0]
    con_empresa = (empresas.where("Sector is not null").select("Sector").distinct()
                   .withColumn("_con_empresa", F.lit(True)))
    return (sin_precios.join(con_empresa, "Sector", "left")
            .withColumn("Motivo",
                        F.when(F.col("_con_empresa").isNull(), "Sector sin empresas en el IBEX")
                         .when(F.col("Fecha") < F.lit(primera), "Anterior al primer precio")
                         .otherwise("Día sin sesión (festivo)"))
            .drop("_con_empresa"))


# Usado en: Ej11
def sectores_sin_empresas(sentimiento, empresas):
    """Sectores que tienen sentimiento pero ninguna empresa en la dimensión."""
    con_empresa = empresas.where("Sector is not null").select("Sector").distinct()
    filas = sentimiento.select("Sector").distinct().join(con_empresa, "Sector", "left_anti").collect()
    return sorted(r["Sector"] for r in filas)
