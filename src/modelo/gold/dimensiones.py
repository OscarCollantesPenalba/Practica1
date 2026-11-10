from pyspark.sql import functions as F


# Usado en: Ej10
def construir_dim_empresa(empresas):
    """Dimensión de empresas: una fila por Ticker con su sector, industria, país y datos de analistas."""
    return empresas


# Usado en: Ej10
def construir_dim_fecha(hecho):
    """Una fila por día con cotizaciones, con atributos para filtrar y agrupar en el dashboard."""
    # Herramienta IA usada: Claude
    
    return (hecho.select("Fecha").distinct()
            .withColumn("Anio", F.year("Fecha"))
            .withColumn("Trimestre", F.quarter("Fecha"))
            .withColumn("Mes", F.month("Fecha"))
            .withColumn("Semana", F.weekofyear("Fecha"))
            .orderBy("Fecha"))