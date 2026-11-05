"""Comprobaciones de calidad y comparación de DataFrames."""
from pyspark.sql import functions as F


def contar_nulos(df, columna):
    """Número de filas en las que la columna es nula."""
    return df.filter(F.col(columna).isNull()).count()


def diferencia_filas(df_a, df_b):
    """Filas que hay en df_a y no en df_b, y al revés (solo columnas comunes, contando duplicados).
    Devuelve (solo_en_a, solo_en_b)."""
    comunes = [c for c in df_a.columns if c in df_b.columns]
    a, b = df_a.select(*comunes), df_b.select(*comunes)
    return a.exceptAll(b).count(), b.exceptAll(a).count()
