"""Comprobaciones de calidad y comparación de DataFrames."""
from functools import reduce

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


def perfil_precios(df):
    """Ej7: radiografía de los precios (bronze/ibex). Devuelve {etiqueta: valor}."""
    columnas_nulos = ["Open", "High", "Low", "Close", "Volume"]
    precios = ["Open", "High", "Low", "Close"]
    # Ruido de float32: un precio cuyo valor cambia al redondearlo a 4 decimales
    ruido = reduce(lambda a, b: a | b, [F.round(c, 4) != F.col(c) for c in precios])
    a = df.agg(
        F.count("*").alias("filas"),
        F.countDistinct("Ticker").alias("tickers"),
        F.min("Date").alias("fecha_min"), F.max("Date").alias("fecha_max"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in columnas_nulos],
        F.sum(ruido.cast("int")).alias("con_ruido"),
        F.sum((F.col("High") < F.col("Low")).cast("int")).alias("high_menor_low"),
        F.sum((F.col("Close") <= 0).cast("int")).alias("close_no_positivo"),
    ).collect()[0].asDict()
    duplicados = a["filas"] - df.select("Ticker", "Date").distinct().count()
    sesiones = df.groupBy("Ticker").count().agg(F.min("count"), F.max("count")).collect()[0]
    return {
        "Filas": a["filas"],
        "Tickers distintos": a["tickers"],
        "Primera / última fecha": f"{a['fecha_min']} / {a['fecha_max']}",
        "Sesiones por ticker (mín / máx)": f"{sesiones[0]} / {sesiones[1]}",
        "Duplicados por (Ticker, Date)": duplicados,
        "Nulos por columna": {c: a[f"nulos_{c}"] for c in columnas_nulos},
        "Filas con algún precio con más de 4 decimales (ruido float32)": a["con_ruido"],
        "Filas con High < Low": a["high_menor_low"],
        "Filas con Close <= 0": a["close_no_positivo"],
    }


def perfil_sentimiento(df):
    """Ej7: radiografía del sentimiento crudo (todo texto). Devuelve {etiqueta: valor}.
    Usa try_cast: con el modo ANSI de Spark 4 un cast normal fallaría con valores mal formados."""
    sector = F.lower(F.trim(F.col("Sector")))
    valor = F.expr("try_cast(regexp_replace(trim(Sentimiento), ',', '.') as double)")
    vacio = lambda c: (F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("int")
    a = df.agg(
        F.count("*").alias("filas"),
        F.min("Fecha").alias("fecha_min"), F.max("Fecha").alias("fecha_max"),
        F.countDistinct("Sector").alias("sectores_crudos"),
        F.countDistinct(sector).alias("sectores_norm"),
        F.sum((F.col("Sector") != F.trim(F.col("Sector"))).cast("int")).alias("espacios"),
        F.sum((F.col("Sector") != F.lower(F.col("Sector"))).cast("int")).alias("mayusculas"),
        F.sum(vacio("Fecha")).alias("fecha_vacia"),
        F.sum(vacio("Sector")).alias("sector_vacio"),
        F.sum(vacio("Sentimiento")).alias("sent_vacio"),
        F.sum((valor.isNull() & (vacio("Sentimiento") == 0)).cast("int")).alias("no_numerico"),
        F.sum(F.col("Sentimiento").contains(",").cast("int")).alias("con_coma"),
        F.min(valor).alias("sent_min"), F.max(valor).alias("sent_max"),
    ).collect()[0].asDict()
    duplicados = a["filas"] - df.select("Fecha", sector.alias("s")).distinct().count()
    por_sector = {r["s"]: r["n"] for r in
                  df.select(sector.alias("s")).groupBy("s").agg(F.count("*").alias("n")).orderBy("s").collect()}
    return {
        "Filas": a["filas"],
        "Primera / última fecha (texto)": f"{a['fecha_min']} / {a['fecha_max']}",
        "Sectores distintos (tal cual / normalizados)": f"{a['sectores_crudos']} / {a['sectores_norm']}",
        "Sector con espacios sobrantes / con mayúsculas": f"{a['espacios']} / {a['mayusculas']}",
        "Vacíos (Fecha / Sector / Sentimiento)": f"{a['fecha_vacia']} / {a['sector_vacio']} / {a['sent_vacio']}",
        "Sentimiento no numérico / con coma decimal": f"{a['no_numerico']} / {a['con_coma']}",
        "Sentimiento mín / máx": f"{a['sent_min']} / {a['sent_max']}",
        "Duplicados por (Fecha, sector normalizado)": duplicados,
        "Filas por sector normalizado": por_sector,
    }


def claves_con_nulo(df, clave, columna):
    """Valores de 'clave' (por ejemplo Ticker) cuya 'columna' (por ejemplo Sector) es nula."""
    return sorted(r[clave] for r in df.filter(F.col(columna).isNull()).select(clave).collect())


def integridad_estrella(hecho, dim_empresa, dim_fecha):
    """Ej13: comprueba que el modelo en estrella es consistente antes de exportarlo.

    Para que Power BI cree relaciones limpias (varios a uno, sin filas en blanco) cada clave del hecho
    debe existir en su dimensión y las claves de las dimensiones no pueden repetirse. Todo debe ser 0.
    """
    def duplicados(df, claves):
        return df.count() - df.select(*claves).distinct().count()

    return {
        "Tickers del hecho sin fila en dim_empresa":
            hecho.select("Ticker").distinct().join(dim_empresa.select("Ticker"), "Ticker", "left_anti").count(),
        "Fechas del hecho sin fila en dim_fecha":
            hecho.select("Fecha").distinct().join(dim_fecha.select("Fecha"), "Fecha", "left_anti").count(),
        "Ticker repetido en dim_empresa": duplicados(dim_empresa, ["Ticker"]),
        "Fecha repetida en dim_fecha": duplicados(dim_fecha, ["Fecha"]),
        "(Ticker, Fecha) repetido en el hecho": duplicados(hecho, ["Ticker", "Fecha"]),
    }
