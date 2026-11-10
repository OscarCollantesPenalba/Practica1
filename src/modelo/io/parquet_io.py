"""Lectura y escritura en el data lake (Parquet). Las de Delta están en delta_io.py (básicas) y bronze/delta_ops.py (Clase 2)."""
from pathlib import Path


# Usado en: Ej1b, Ej2b, Ej2c
def escribir_parquet(df, ruta, modo="overwrite"):
    """Escribe un DataFrame en Parquet. modo: overwrite | append | ignore | error."""
    df.write.mode(modo).parquet(ruta)


# Usado en: Ej1c
def leer_parquet(spark, ruta, columnas=None):
    """Lee Parquet; si se indican columnas, solo se leen esas (column pruning)."""
    df = spark.read.parquet(ruta)
    return df.select(*columnas) if columnas else df


# Usado en: Ej2b, Ej2c
def contar_filas(spark, ruta):
    """Número de filas de una carpeta Parquet (0 si todavía no existe)."""
    if not Path(ruta).exists():
        return 0
    return spark.read.parquet(ruta).count()
