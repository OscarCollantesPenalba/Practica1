from pyspark.sql import functions as F


# Usado en: Ej11
def precios_sin_sentimiento(precios, empresas, sentimiento):
    """(a) Cotizaciones cuyo (Fecha, Sector) no tiene sentimiento. Anti-join: filas de la izquierda
    (precios con el sector de su empresa) sin pareja en la derecha (sentimiento)."""
    con_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker", "left")
    return con_sector.join(sentimiento.select("Fecha", "Sector"), ["Fecha", "Sector"], "left_anti")


# Usado en: Ej11
def sentimiento_sin_precios(precios, empresas, sentimiento):
    """(b) Filas de sentimiento cuyo (Fecha, Sector) no tiene ninguna cotización."""
    fechas_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker").select("Fecha", "Sector").distinct()
    return sentimiento.join(fechas_sector, ["Fecha", "Sector"], "left_anti")


# Usado en: Ej11
def resumen_por_sector(df):
    """Por cada sector: cuántas filas no cruzan y entre qué fechas (Desde, Hasta)."""
    # Herramienta IA usada: Claude
    
    return (df.groupBy("Sector")
            .agg(F.count("*").alias("Filas"), F.min("Fecha").alias("Desde"), F.max("Fecha").alias("Hasta"))
            .orderBy("Sector"))