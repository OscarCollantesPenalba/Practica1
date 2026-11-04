# Práctica 1 - Data Lake con Delta y limpieza de datos (IBEX-35)

Data lake por capas (bronze, silver, gold) con Delta Lake sobre ficheros locales,
construido con PySpark 4.0.1 en modo local.

## Requisitos
- Entorno conda `aplicacion2026` (Python 3.11, Java 17, pyspark 4.0.1, delta-spark 4.0.1, yfinance)
- Windows: `winutils.exe` y `hadoop.dll` en `C:\hadoop\bin` y variable `HADOOP_HOME`

## Ejecución (desde la raíz del proyecto)
    conda activate aplicacion2026
    python main.py                # todos los ejercicios implementados
    python main.py ej1a           # solo los indicados

La primera ejecución descarga los .jar de Delta (hace falta internet). El data lake se genera en
`data/lake/` (no se sube a git; se regenera ejecutando el proyecto). Si la ruta del proyecto tiene
espacios o tildes y Spark da error de rutas, se puede mover el lago con la variable `LAKE_DIR`.

## Estructura (MVC)
- `main.py`: punto de entrada
- `src/modelo/`: lógica reutilizable (`config`, `spark_session`, `yahoo_finance`...)
- `src/vista/`: salida por consola
- `src/controlador/`: un método por ejercicio (`ej1a`, `ej1b`...) que llama al modelo
- `data/`: datos de entrada y data lake generado (el lago no se sube a git)
- `lib/`: conector JDBC de MySQL (`.jar`)

## Tickers
35 valores del IBEX-35 (composición vigente desde el 9/07/2024, cuando Puig sustituyó a Meliá).
