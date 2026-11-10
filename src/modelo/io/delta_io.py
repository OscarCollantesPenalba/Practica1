"""Lectura y escritura básica de tablas Delta. No sabe de capas: recibe una ruta y un DataFrame.

Las operaciones propias de la Clase 2 (convertir, MERGE, DELETE, RESTORE, time travel) están en
src/modelo/bronze/delta_ops.py.
"""
from delta.tables import DeltaTable


# Usado en: Ej4, Ej5a, Ej5b, Ej5c, Ej5d, Ej6, Ej7, Ej8, Ej9, Ej10, Ej11, Ej12, Ej13
def leer_delta(spark, ruta, condicion=None):
    """Lee la versión actual de una tabla Delta (opcionalmente filtrada por una condición SQL)."""
    df = spark.read.format("delta").load(ruta)
    return df.filter(condicion) if condicion else df


# Usado en: Ej5a, Ej5b, Ej8, Ej9, Ej10, Ej12
def escribir_delta(df, ruta, modo="append", merge_schema=False, overwrite_schema=False):
    """Escribe un DataFrame en una tabla Delta. Con merge_schema=True permite que la tabla
    incorpore columnas nuevas (evolución de esquema); sin él, Delta valida el esquema.
    Con overwrite_schema=True (solo con modo overwrite) la tabla se reconstruye con el esquema
    del DataFrame; sirve para regenerar silver y gold sin perder el historial de versiones."""
    escritor = df.write.format("delta").mode(modo)
    if merge_schema:
        escritor = escritor.option("mergeSchema", "true")
    if overwrite_schema:
        escritor = escritor.option("overwriteSchema", "true")
    escritor.save(ruta)


# Usado en: Ej4, Ej5a, Ej5b, Ej5c, Ej8, Ej10
def contar_filas_delta(spark, ruta, condicion=None):
    """Número de filas de la versión actual de una tabla Delta (opcionalmente solo las que
    cumplen una condición SQL, por ejemplo "Ticker = 'BBVA.MC'")."""
    return leer_delta(spark, ruta, condicion).count()


# Usado en: Ej4, Ej5a, Ej5b, Ej5c, Ej5d, Ej6, Ej12
def version_actual(spark, ruta):
    """Número de la versión actual de la tabla (un entero). No modifica nada."""
    return DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]
