from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from src.modelo.io.delta_io import escribir_delta


# Usado en: Ej3
def convertir_a_delta(spark, ruta):
    """Convierte una carpeta Parquet en tabla Delta sin reescribir los datos."""
    return DeltaTable.convertToDelta(spark, f"parquet.`{ruta}`")


# Usado en: Ej3, Ej5d, Ej6
def historial(spark, ruta):
    """Historial de versiones (DataFrame, de la más reciente a la más antigua)."""
    return DeltaTable.forPath(spark, ruta).history()


# Usado en: Ej4
def merge_insertar_nuevos(spark, ruta, nuevos):
    """MERGE que inserta solo las filas de 'nuevos' cuya clave (Ticker, Date) no existe en la tabla."""
    destino = DeltaTable.forPath(spark, ruta)
    (destino.alias("d")
     .merge(nuevos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
     .whenNotMatchedInsertAll()
     .execute())


# Usado en: Ej5a, Ej5b
def con_close_mas5(df):
    """Añade la columna Close_mas5 (Close + 5), que la tabla no tiene."""
    return df.withColumn("Close_mas5", F.col("Close") + 5)


# Usado en: Ej5a, Ej5b
def intentar_escribir_delta(df, ruta, modo="append", merge_schema=False):
    """Como escribir_delta, pero si Delta rechaza la escritura devuelve la primera línea del
    error en lugar de romper el programa. Si la acepta, devuelve None."""
    # Herramienta IA usada: Claude
    
    try:
        escribir_delta(df, ruta, modo, merge_schema)
    except AnalysisException as error:
        return str(error).strip().splitlines()[0]
    return None


# Usado en: Ej5c
def borrar_filas(spark, ruta, condicion):
    """DELETE de las filas que cumplen la condición (crea una versión nueva en el log)."""
    DeltaTable.forPath(spark, ruta).delete(condicion)


# Usado en: Ej5d
def leer_delta_version(spark, ruta, version):
    """Time travel: lee una versión antigua. Solo lee, la tabla actual no cambia."""
    return spark.read.format("delta").option("versionAsOf", version).load(ruta)


# Usado en: Ej6
def version_de_operacion(spark, ruta, operacion):
    """Última versión cuya operación es 'operacion' (por ejemplo "MERGE"), o None si no hay ninguna."""
    # Herramienta IA usada: Claude
    
    return (historial(spark, ruta)
            .filter(F.col("operation") == operacion)
            .agg(F.max("version")).collect()[0][0])


# Usado en: Ej6
def restaurar_version(spark, ruta, version):
    """RESTORE: deja la tabla como estaba en esa versión. Crea una versión nueva, no borra el historial."""
    DeltaTable.forPath(spark, ruta).restoreToVersion(version)