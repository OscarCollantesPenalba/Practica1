from pyspark.sql import functions as F


# Usado en: Ej7, Ej12
def contar_nulos(df, columna):
    """Número de filas en las que la columna es nula."""
    return df.filter(F.col(columna).isNull()).count()


# Usado en: Ej7, Ej13
def contar_duplicados(df, claves):
    """Filas sobrantes: las que tiene el DataFrame de más respecto a quitar duplicados por 'claves'."""
    return df.count() - df.dropDuplicates(claves).count()


# Usado en: Ej7
def nulos_por_columna(df):
    """Diccionario {columna: nº de nulos}."""
    return {c: contar_nulos(df, c) for c in df.columns}


# Usado en: Ej7
def perfil_precios(df):
    """Radiografía de los precios (bronze/ibex). Devuelve {etiqueta: valor}."""
    # Herramienta IA usada: Claude
    
    fechas = df.agg(F.min("Date"), F.max("Date")).collect()[0]
    return {
        "Filas": df.count(),
        "Empresas": df.select("Ticker").distinct().count(),
        "Primera / última fecha": f"{fechas[0]} / {fechas[1]}",
        "Duplicados por (Ticker, Date)": contar_duplicados(df, ["Ticker", "Date"]),
        "Nulos por columna": nulos_por_columna(df),
        "Filas con High < Low": df.filter(F.col("High") < F.col("Low")).count(),
        "Filas con Close <= 0": df.filter(F.col("Close") <= 0).count(),
    }


# Usado en: Ej7
def perfil_sentimiento(df):
    """Radiografía del sentimiento crudo (bronze/sentimiento). Devuelve {etiqueta: valor}."""
    # Herramienta IA usada: Claude
    
    fechas = df.agg(F.min("Fecha"), F.max("Fecha")).collect()[0]
    return {
        "Filas": df.count(),
        "Primera / última fecha (texto)": f"{fechas[0]} / {fechas[1]}",
        "Nulos por columna": nulos_por_columna(df),
        "Sentimiento con coma decimal": df.filter(F.col("Sentimiento").contains(",")).count(),
        "Duplicados por (Fecha, Sector)": contar_duplicados(df, ["Fecha", "Sector"]),
    }


# Usado en: Ej13
def integridad_estrella(hecho, dim_empresa, dim_fecha):
    """Ej13: comprueba que el modelo en estrella es consistente antes de exportarlo.

    Para que Power BI cree relaciones limpias (varios a uno, sin filas en blanco) cada clave del hecho
    debe existir en su dimensión y las claves de las dimensiones no pueden repetirse. Todo debe ser 0.
    """
    return {
        "Tickers del hecho sin fila en dim_empresa":
            hecho.select("Ticker").distinct().join(dim_empresa.select("Ticker"), "Ticker", "left_anti").count(),
        "Fechas del hecho sin fila en dim_fecha":
            hecho.select("Fecha").distinct().join(dim_fecha.select("Fecha"), "Fecha", "left_anti").count(),
        "Ticker repetido en dim_empresa": contar_duplicados(dim_empresa, ["Ticker"]),
        "Fecha repetida en dim_fecha": contar_duplicados(dim_fecha, ["Fecha"]),
        "(Ticker, Fecha) repetido en el hecho": contar_duplicados(hecho, ["Ticker", "Fecha"]),
    }