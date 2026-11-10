from delta.tables import DeltaTable


# Usado en: Ej5a a Ej13
def leer_delta(spark, ruta, condicion=None):
    """Lee la versión actual de una tabla Delta (opcionalmente filtrada con una condición SQL)."""
    df = spark.read.format("delta").load(ruta)
    return df.filter(condicion) if condicion else df


# Usado en: Ej5a, Ej5b, Ej8, Ej9, Ej10, Ej12
def escribir_delta(df, ruta, modo="append", merge_schema=False, overwrite_schema=False):
    """Escribe un DataFrame en una tabla Delta"""
    
    escritor = df.write.format("delta").mode(modo)
    if merge_schema:
        escritor = escritor.option("mergeSchema", "true")
    if overwrite_schema:
        escritor = escritor.option("overwriteSchema", "true")
    escritor.save(ruta)


# Usado en: Ej4, Ej5a, Ej5b, Ej5c, Ej6, Ej8, Ej10
def contar_filas_delta(spark, ruta, condicion=None):
    """Filas de la versión actual (opcionalmente solo las que cumplen una condición SQL)."""
    return leer_delta(spark, ruta, condicion).count()


# Usado en: Ej4, Ej5a, Ej5b, Ej6, Ej12
def version_actual(spark, ruta):
    """Número de la versión actual de la tabla (un entero). No modifica nada."""
    return DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]