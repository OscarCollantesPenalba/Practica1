"""Gold - dimensiones del modelo en estrella: dim_empresa y dim_fecha.

Usado en: Ej10.
"""
from pyspark.sql import functions as F

MESES = "'Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'"
DIAS = "'Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo'"


# Usado en: Ej10
def construir_dim_empresa(empresas):
    """Dimensión de empresas: una fila por Ticker con su sector, industria, país y datos de analistas."""
    return empresas


# Usado en: Ej10
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
