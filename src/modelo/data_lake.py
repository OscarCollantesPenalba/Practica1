"""Lectura y escritura en el data lake (Parquet). Las funciones de Delta irán en delta_ops.py."""
from pathlib import Path


def escribir_parquet(df, ruta, modo="overwrite"):
    """Escribe un DataFrame en Parquet. modo: overwrite | append | ignore | error."""
    df.write.mode(modo).parquet(ruta)


def leer_parquet(spark, ruta, columnas=None):
    """Lee Parquet; si se indican columnas, solo se leen esas (column pruning)."""
    df = spark.read.parquet(ruta)
    return df.select(*columnas) if columnas else df


def contar_filas(spark, ruta):
    """Número de filas de una carpeta Parquet (0 si todavía no existe)."""
    if not Path(ruta).exists():
        return 0
    return spark.read.parquet(ruta).count()
