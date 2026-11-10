from pathlib import Path

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from src.modelo.io.delta_io import escribir_delta


# ===================== Ej3: convertir Parquet a Delta e historial =====================

# Usado en: Ej3
def convertir_a_delta(spark, ruta):
    """Ej3: convierte una carpeta Parquet en tabla Delta SIN reescribir los datos."""
    return DeltaTable.convertToDelta(spark, f"parquet.`{ruta}`")


# Usado en: Ej3, Ej5d, Ej6
def historial(spark, ruta):
    """Historial de versiones de una tabla Delta (DataFrame, de la más reciente a la más antigua)."""
    return DeltaTable.forPath(spark, ruta).history()


# ===================== Ej4: MERGE =====================

# Usado en: Ej4
def merge_insertar_nuevos(spark, ruta, nuevos):
    """Ej4: MERGE que inserta solo las filas de 'nuevos' cuya clave (Ticker, Date) no existe en la tabla."""
    destino = DeltaTable.forPath(spark, ruta)
    (destino.alias("d")
     .merge(nuevos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
     .whenNotMatchedInsertAll()
     .execute())


# ===================== Ej5a-Ej5b: schema enforcement y evolución de esquema =====================

# Usado en: Ej5a, Ej5b
def con_close_mas5(df):
    """Ej5a: añade la columna Close_mas5 (Close + 5). Es una columna que la tabla NO tiene."""
    return df.withColumn("Close_mas5", F.col("Close") + 5)

# Usado en: Ej5a, Ej5b
def intentar_escribir_delta(df, ruta, modo="append", merge_schema=False):
    """Como escribir_delta, pero si Delta rechaza la escritura (error de esquema) devuelve el
    mensaje del error en lugar de romper el programa. Si la acepta, devuelve None."""
    # Herramienta IA usada: Claude
    
    try:
        escribir_delta(df, ruta, modo, merge_schema)
    except AnalysisException as error:
        lineas = [l.strip() for l in str(error).splitlines() if l.strip()]
        return False, (lineas[0] if lineas else type(error).__name__)
    return True, ""

 
# ===================== Ej5c: DELETE =====================

# Usado en: Ej5c
def borrar_filas(spark, ruta, condicion):
    """Ej5c: DELETE de las filas que cumplen la condición. Crea una versión nueva en el log; los
    ficheros afectados dejan de formar parte de la tabla pero siguen en disco hasta un VACUUM."""
    DeltaTable.forPath(spark, ruta).delete(condicion)


# ===================== Ej5d y Ej6: time travel y RESTORE =====================

# Usado en: Ej5d, Ej6
def leer_delta_version(spark, ruta, version):
    """Lee una versión antigua de la tabla (time travel). Solo LEE: la tabla actual no cambia."""
    return spark.read.format("delta").option("versionAsOf", version).load(ruta)


# Usado en: Ej5d, Ej6
def version_de_operacion(spark, ruta, operacion):
    """Última versión cuya operación es 'operacion' (por ejemplo "MERGE"), o None si no hay ninguna."""
    return (historial(spark, ruta)
            .filter(F.col("operation") == operacion)
            .agg(F.max("version")).collect()[0][0])


# Usado en: Ej6
def restaurar_version(spark, ruta, version):
    """Ej6: deja la tabla como estaba en la versión indicada (RESTORE). No borra el historial:
    crea un commit nuevo cuyo contenido es idéntico al de esa versión."""
    DeltaTable.forPath(spark, ruta).restoreToVersion(version)
