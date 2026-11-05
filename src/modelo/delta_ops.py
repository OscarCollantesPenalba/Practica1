"""Operaciones de Delta Lake sobre las tablas del data lake."""
from pathlib import Path

from delta.tables import DeltaTable


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


def contar_filas_delta(spark, ruta):
    """Número de filas de la versión actual de una tabla Delta."""
    return spark.read.format("delta").load(ruta).count()


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
