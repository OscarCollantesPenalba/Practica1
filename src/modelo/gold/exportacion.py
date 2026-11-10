# Usado en: Ej13
def exportar_parquet(df, ruta, ordenar_por):
    """Escribe el DataFrame como Parquet en la carpeta 'ruta', en UN solo fichero y ordenado por la clave.

    - Un único fichero: las tablas son pequeñas; particionar generaría muchos ficheros diminutos,
      más lentos de abrir que uno solo.
    - Ordenado por la clave: los valores parecidos quedan juntos, el fichero comprime mejor y las
      estadísticas mín/máx de cada columna permiten saltarse datos al filtrar.
    Compresión: Snappy (la de Spark por defecto, rápida de leer)."""
    # Herramienta IA usada: Claude
    
    df.coalesce(1).sortWithinPartitions(*ordenar_por).write.mode("overwrite").parquet(ruta)