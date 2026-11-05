"""Operaciones de Delta Lake sobre las tablas del data lake."""
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F


def convertir_a_delta(spark, ruta):
    """Ej3: convierte una carpeta Parquet en tabla Delta SIN reescribir los datos.

    Solo añade la carpeta _delta_log con el commit 0 (operación de metadatos). Los
    backticks en la ruta son obligatorios. Si la carpeta ya es Delta, no hace nada.
    """
    return DeltaTable.convertToDelta(spark, f"parquet.`{ruta}`")


def historial(spark, ruta):
    """Historial de versiones de una tabla Delta (DataFrame, de la más reciente a la más antigua)."""
    return DeltaTable.forPath(spark, ruta).history()


def ficheros_delta_log(ruta):
    """Nombres de los ficheros que hay dentro de _delta_log (lista vacía si no existe)."""
    log = Path(ruta) / "_delta_log"
    return sorted(p.name for p in log.iterdir()) if log.is_dir() else []


def version_actual(spark, ruta):
    """Número de la versión actual de la tabla (un entero). No modifica nada."""
    return DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]


def contar_filas_delta(spark, ruta, condicion=None):
    """Número de filas de la versión actual de una tabla Delta (opcionalmente solo las que
    cumplen una condición SQL, por ejemplo "Ticker = 'BBVA.MC'")."""
    return leer_delta(spark, ruta, condicion).count()


def merge_insertar_nuevos(spark, ruta, nuevos, claves):
    """Ej4: MERGE que inserta solo las filas de 'nuevos' cuya clave no existe ya en la tabla.

    Las filas que ya existen se ignoran, así que la carga es idempotente: ejecutarla dos
    veces no duplica datos (a diferencia de un append).
    """
    destino = DeltaTable.forPath(spark, ruta)
    condicion = " AND ".join(f"d.{c} = n.{c}" for c in claves)
    (destino.alias("d")
     .merge(nuevos.alias("n"), condicion)
     .whenNotMatchedInsertAll()
     .execute())


def leer_delta(spark, ruta, condicion=None):
    """Lee la versión actual de una tabla Delta (opcionalmente filtrada por una condición SQL)."""
    df = spark.read.format("delta").load(ruta)
    return df.filter(condicion) if condicion else df


def con_close_mas5(df):
    """Ej5a: añade la columna Close_mas5 (Close + 5). Es una columna que la tabla NO tiene."""
    return df.withColumn("Close_mas5", F.col("Close") + 5)


def escribir_delta(df, ruta, modo="append", merge_schema=False):
    """Escribe un DataFrame en una tabla Delta. Con merge_schema=True permite que la tabla
    incorpore columnas nuevas (evolución de esquema); sin él, Delta valida el esquema."""
    escritor = df.write.format("delta").mode(modo)
    if merge_schema:
        escritor = escritor.option("mergeSchema", "true")
    escritor.save(ruta)


def intentar_escribir_delta(df, ruta, modo="append", merge_schema=False):
    """Como escribir_delta, pero si Delta rechaza la escritura (error de esquema) devuelve
    (False, primera línea del error) en lugar de romper el programa. Si la acepta: (True, "")."""
    try:
        escribir_delta(df, ruta, modo, merge_schema)
    except AnalysisException as error:
        lineas = [l.strip() for l in str(error).splitlines() if l.strip()]
        return False, (lineas[0] if lineas else type(error).__name__)
    return True, ""


def borrar_filas(spark, ruta, condicion):
    """Ej5c: DELETE de las filas que cumplen la condición. Crea una versión nueva en el log; los
    ficheros afectados dejan de formar parte de la tabla pero siguen en disco hasta un VACUUM."""
    DeltaTable.forPath(spark, ruta).delete(condicion)


def leer_delta_version(spark, ruta, version):
    """Lee una versión antigua de la tabla (time travel). Solo LEE: la tabla actual no cambia."""
    return spark.read.format("delta").option("versionAsOf", version).load(ruta)


def version_de_operacion(spark, ruta, operacion):
    """Última versión cuya operación es 'operacion' (por ejemplo "MERGE"), o None si no hay ninguna."""
    return (historial(spark, ruta)
            .filter(F.col("operation") == operacion)
            .agg(F.max("version")).collect()[0][0])


def restaurar_version(spark, ruta, version):
    """Ej6: deja la tabla como estaba en la versión indicada (RESTORE). No borra el historial:
    crea un commit nuevo cuyo contenido es idéntico al de esa versión."""
    DeltaTable.forPath(spark, ruta).restoreToVersion(version)
