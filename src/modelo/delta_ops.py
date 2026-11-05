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
