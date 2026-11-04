# Práctica 1 - Data Lake con Delta y limpieza de datos (IBEX-35)

Data lake por capas (bronze, silver, gold) con Delta Lake sobre ficheros locales,
construido con PySpark 4.0.1 en modo local.

## Requisitos
- Entorno conda `aplicacion2026` (Python 3.11, Java 17, pyspark 4.0.1, delta-spark 4.0.1, yfinance)
- Windows: `winutils.exe` y `hadoop.dll` en `C:\hadoop\bin` y variable `HADOOP_HOME`

## Estructura (MVC)
- `main.py`: punto de entrada
- `src/modelo/`: lógica reutilizable (descarga, limpieza, Delta, integración)
- `src/vista/`: salida por consola
- `src/controlador/`: un método por ejercicio que llama al modelo
- `data/`: datos de entrada y data lake generado (el lago no se sube a git)
- `lib/`: conector JDBC de MySQL (`.jar`)
