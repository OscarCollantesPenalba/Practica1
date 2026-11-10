"""Ej13: exportación de la capa gold a Parquet, lista para Power BI."""
import os
import shutil
from pathlib import Path


# Usado en: Ej13
def exportar_parquet_unico(df, destino, ordenar_por):
    """Escribe el DataFrame como UN solo fichero Parquet limpio (destino = ruta del .parquet).

    - Un único fichero: las tablas son pequeñas (decenas de miles de filas como mucho); particionar
      generaría muchos ficheros diminutos, más lentos de abrir que uno solo.
    - Ordenado por la clave: los valores parecidos quedan juntos, el fichero comprime mejor y las
      estadísticas mín/máx de cada columna permiten saltarse datos al filtrar.
    - Limpio: Spark escribe una carpeta con part-*.parquet, _SUCCESS y ficheros .crc. Aquí se deja
      solo el .parquet, que es lo que abre Power BI.
    Compresión: Snappy (la de Spark por defecto, rápida de leer).
    """
    destino = Path(destino)
    temporal = destino.parent / f"_tmp_{destino.stem}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    (df.coalesce(1).sortWithinPartitions(*ordenar_por)
       .write.mode("overwrite").parquet(temporal.as_posix()))
    partes = list(temporal.glob("part-*.parquet"))
    if len(partes) != 1:
        raise RuntimeError(f"Se esperaba un único fichero Parquet y hay {len(partes)} en {temporal}")
    os.replace(partes[0], destino)
    shutil.rmtree(temporal, ignore_errors=True)


# Usado en: Ej13
def tamano_kb(fichero):
    """Tamaño del fichero en KB."""
    return round(Path(fichero).stat().st_size / 1024, 1)
