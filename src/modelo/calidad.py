"""Comprobaciones de calidad y comparación de DataFrames."""
from pyspark.sql import functions as F


def contar_nulos(df, columna):
    """Número de filas en las que la columna es nula."""
    return df.filter(F.col(columna).isNull()).count()
