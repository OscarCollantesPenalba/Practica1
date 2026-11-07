"""Capa gold: integración de las fuentes en un modelo en estrella (un hecho y sus dimensiones)."""
from pyspark.sql import functions as F

MESES = "'Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'"
DIAS = "'Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo'"


def construir_hecho(precios, empresas, sentimiento):
    """Ej10: tabla de hechos. Una fila por (Ticker, Fecha): precios + sentimiento del sector de la empresa.

    El sector se obtiene de la dimensión de empresas y se usa solo para cruzar con el sentimiento;
    no se guarda en el hecho (se alcanza desde dim_empresa). Es un left join: ninguna cotización se
    pierde; si no hay sentimiento para su sector y fecha, Sentimiento queda nulo.
    """
    sectores = empresas.select("Ticker", "Sector")
    return (precios
            .join(sectores, "Ticker", "left")
            .join(sentimiento, ["Fecha", "Sector"], "left")
            .select("Fecha", "Ticker", "Open", "High", "Low", "Close", "Volume",
                    "Dividends", "Stock_Splits", "Sentimiento"))


def construir_dim_empresa(empresas):
    """Dimensión de empresas: una fila por Ticker con su sector, industria, país y datos de analistas."""
    return empresas


def construir_dim_fecha(hecho):
    """Dimensión de fechas: calendario continuo (también fines de semana) desde la primera hasta la
    última fecha del hecho, con los atributos para filtrar y agrupar en el dashboard."""
    limites = hecho.agg(F.min("Fecha").alias("a"), F.max("Fecha").alias("b")).collect()[0]
    fechas = hecho.sparkSession.range(1).select(
        F.explode(F.sequence(F.lit(limites["a"]), F.lit(limites["b"]), F.expr("interval 1 day"))).alias("Fecha"))
    return fechas.select(
        "Fecha",
        F.year("Fecha").alias("Anio"),
        F.quarter("Fecha").alias("Trimestre"),
        F.month("Fecha").alias("Mes"),
        F.expr(f"element_at(array({MESES}), month(Fecha))").alias("Nombre_Mes"),
        F.date_format("Fecha", "yyyy-MM").alias("Anio_Mes"),
        F.weekofyear("Fecha").alias("Semana_ISO"),
        F.expr("weekday(Fecha) + 1").alias("Dia_Semana"),
        F.expr(f"element_at(array({DIAS}), weekday(Fecha) + 1)").alias("Nombre_Dia"),
        F.expr("weekday(Fecha) >= 5").alias("Es_Fin_De_Semana"),
    )


def precios_sin_sentimiento(precios, empresas, sentimiento):
    """Ej11 (a): cotizaciones cuyo (Fecha, Sector) no tiene sentimiento. Anti-join: filas de la
    izquierda (precios con el sector de su empresa) sin pareja en la derecha (sentimiento)."""
    con_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker", "left")
    return con_sector.join(sentimiento.select("Fecha", "Sector"), ["Fecha", "Sector"], "left_anti")


def sentimiento_sin_precios(precios, empresas, sentimiento):
    """Ej11 (b): filas de sentimiento cuyo (Fecha, Sector) no tiene ninguna cotización."""
    fechas_sector = precios.join(empresas.select("Ticker", "Sector"), "Ticker").select("Fecha", "Sector").distinct()
    return sentimiento.join(fechas_sector, ["Fecha", "Sector"], "left_anti")


def con_motivo_precios(sin_sentimiento, sentimiento):
    """Añade la columna Motivo a las cotizaciones sin sentimiento (a)."""
    ultima = sentimiento.agg(F.max("Fecha")).collect()[0][0]
    return sin_sentimiento.withColumn(
        "Motivo",
        F.when(F.col("Sector").isNull(), "Empresa sin sector")
         .when(F.col("Fecha") > F.lit(ultima), "Posterior al último dato del experto")
         .otherwise("Sin dato del experto ese día"))


def con_motivo_sentimiento(sin_precios, precios, empresas):
    """Añade la columna Motivo al sentimiento sin precios (b)."""
    primera = precios.agg(F.min("Fecha")).collect()[0][0]
    con_empresa = (empresas.where("Sector is not null").select("Sector").distinct()
                   .withColumn("_con_empresa", F.lit(True)))
    return (sin_precios.join(con_empresa, "Sector", "left")
            .withColumn("Motivo",
                        F.when(F.col("_con_empresa").isNull(), "Sector sin empresas en el IBEX")
                         .when(F.col("Fecha") < F.lit(primera), "Anterior al primer precio")
                         .otherwise("Día sin sesión (festivo)"))
            .drop("_con_empresa"))


def sectores_sin_empresas(sentimiento, empresas):
    """Sectores que tienen sentimiento pero ninguna empresa en la dimensión."""
    con_empresa = empresas.where("Sector is not null").select("Sector").distinct()
    filas = sentimiento.select("Sector").distinct().join(con_empresa, "Sector", "left_anti").collect()
    return sorted(r["Sector"] for r in filas)
