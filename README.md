# Práctica 1 - Data Lake con Delta y limpieza de datos (IBEX-35)

Data lake por capas (bronze, silver y gold) con Delta Lake sobre ficheros locales, construido con
PySpark 4.0.1 en modo local. Cruza los precios diarios de las 35 empresas del IBEX-35 (Yahoo Finance)
con un índice de sentimiento por sector (CSV de un analista) y deja la capa gold en un modelo en
estrella, exportado a Parquet para Power BI (Práctica 2).

## Requisitos
- Entorno conda `aplicacion2026`: Python 3.11, Java 17, pyspark 4.0.1, delta-spark 4.0.1, yfinance.
- Windows: `winutils.exe` y `hadoop.dll` en `C:\hadoop\bin` y la variable `HADOOP_HOME`.
- Conexión a internet: Yahoo Finance y, la primera vez, los `.jar` de Delta.

## Ejecución
Desde la raíz del proyecto y con el entorno activado:

    conda activate aplicacion2026
    python main.py                       # todos los ejercicios, en orden
    python main.py ej1a ej2a             # solo los indicados
    python main.py ej11 ej12 ej13        # los ejercicios se ejecutan en el orden escrito

`python main.py` reconstruye el data lake desde cero y tarda varios minutos (casi todo es la descarga
desde Yahoo). Cada ejercicio imprime su nombre (`Ej1a`, `Ej1b`...) y solo lo que pide el enunciado.

Los ejercicios dependen unos de otros: respetar el orden (bronze -> Delta -> silver -> gold -> export).

### Avisos
- **Los datos llegan "hasta hoy"** (Ej4), por lo que las cifras cambian cada día que se ejecuta.
- **`ej1b`, `ej2b` y `ej2c` reescriben bronze en Parquet y borran su `_delta_log`.** Una vez hecho el
  Ej3, ejecutarlos sueltos hace perder las versiones de Delta. `python main.py` completo es seguro porque
  vuelve a convertir a Delta.
- **Ej5b duplica la tabla** (escribe la tabla entera con la columna nueva) hasta que el Ej6 la restaura.
  No repetir `ej5a` después de `ej5b`: con la columna ya en el esquema, Delta aceptaría el append.
- **`ej12` modifica `hecho_cotizacion`**: ejecutar antes `ej10`. Si se vuelve a ejecutar `ej10`, el hecho
  se reconstruye sin las columnas calculadas y hay que repetir `ej12`.
- Al cerrar Spark en Windows aparece un error `ShutdownHookManager ... Failed to delete`: es la limpieza de
  ficheros temporales, es inofensivo y no afecta a los resultados.
- Si la ruta del proyecto tiene espacios o tildes y Spark da error de rutas, se puede mover el lago con la
  variable de entorno `LAKE_DIR`.

## Estructura (MVC)
    main.py                          punto de entrada
    src/controlador/ibex_controller.py   un método por ejercicio (ej1a ... ej13); solo llama al modelo y a la vista
    src/vista/ibex_view.py               imprime por consola; no calcula nada
    src/modelo/
        config.py                    rutas del lago, periodo, tickers, sectores en español
        spark_session.py             sesión de Spark local con Delta
        yahoo_finance.py             descarga de precios y de la info de las empresas
        data_lake.py                 leer y escribir Parquet
        sentimiento.py               lectura del CSV
        delta_ops.py                 Delta: conversión, MERGE, DELETE, RESTORE, time travel, esquema
        limpieza.py                  bronze -> silver
        integracion.py               silver -> gold (hecho y dimensiones) y anti-joins
        indicadores.py               columnas calculadas del hecho
        calidad.py                   perfiles de datos y comprobaciones
        exportacion.py               gold -> Parquet
    data/sentimiento_ibex.csv        datos de entrada (CSV del analista)
    data/lake/                       data lake generado (no se sube a git)
    data/export/gold/                exportación para Power BI (no se sube a git)
    lib/                             conector JDBC de MySQL (reservado; ningún ejercicio lo usa)

La lógica vive en funciones reutilizables del modelo (por ejemplo `escribir_delta` o `leer_delta` se usan
en muchos ejercicios); el controlador solo las encadena.

## Capas del data lake
Las tres capas son tablas Delta, cada una en su carpeta dentro de `data/lake/`.

| Capa | Tabla | Contenido |
|---|---|---|
| bronze | `bronze/ibex` | Precios diarios tal como llegan de Yahoo (35 empresas) |
| bronze | `bronze/sentimiento` | CSV del analista, todo en texto |
| bronze | `bronze/empresas` | Objeto `info` de Yahoo de cada empresa, sin limpiar |
| silver | `silver/ibex` | Precios limpios: `Fecha`, precios a 4 decimales, sin duplicados ni sesiones incoherentes |
| silver | `silver/sentimiento` | `Fecha` (date), `Sector` normalizado, `Sentimiento` (double) |
| silver | `silver/empresas` | Dimensión limpia: sector (inglés y español), industria, país, datos de analistas |
| gold | `gold/hecho_cotizacion` | Hecho: una fila por empresa y día, con precios, sentimiento y columnas calculadas |
| gold | `gold/dim_empresa` | Dimensión: una fila por `Ticker` |
| gold | `gold/dim_fecha` | Dimensión: calendario continuo con año, mes, día de la semana, etc. |

Exportación (Ej13): `data/export/gold/hecho_cotizacion.parquet`, `dim_empresa.parquet` y `dim_fecha.parquet`,
un fichero por tabla, ordenado por su clave y comprimido con Snappy.

## Modelo en estrella
El hecho (`hecho_cotizacion`, claves `Ticker` y `Fecha`) contiene las medidas: `Open`, `High`, `Low`, `Close`,
`Volume`, `Dividends`, `Stock_Splits`, `Sentimiento` y las calculadas (`Variacion_Diaria_Pct`,
`Rango_Intradia_Pct`, `Media_Movil_20`, `Sentimiento_Anterior`, `Tiene_Sentimiento`). Las dimensiones aportan
los atributos para filtrar y agrupar: `dim_empresa` (sector, industria, país...) y `dim_fecha`.

## Ejercicios
| Ejercicio | Método | Qué hace |
|---|---|---|
| Ej1a | `ej1a` | Descarga el histórico de una acción (BBVA) y define el esquema con sus nulos |
| Ej1b | `ej1b` | Descarga las 35 empresas y las guarda en `bronze/ibex` (Parquet) |
| Ej1c | `ej1c` | Lee de vuelta el Parquet y usa column pruning |
| Ej2a | `ej2a` | Lee el CSV de sentimiento: todo llega como texto |
| Ej2b | `ej2b` | Guarda el sentimiento en `bronze/sentimiento` |
| Ej2c | `ej2c` | `append` en Parquet duplica las filas; se restaura con `overwrite` |
| Ej3 | `ej3` | Convierte las dos tablas de bronze a Delta sin reescribir los datos; muestra el historial y `_delta_log` |
| Ej4 | `ej4` | MERGE con los datos nuevos hasta hoy; la segunda ejecución no inserta nada |
| Ej5a | `ej5a` | Schema enforcement: Delta rechaza el append con una columna de más |
| Ej5b | `ej5b` | Con `mergeSchema` Delta lo acepta y añade la columna |
| Ej5c | `ej5c` | DELETE de las filas de BBVA |
| Ej5d | `ej5d` | `history()` y lectura con `versionAsOf` |
| Ej6 | `ej6` | RESTORE de la tabla al estado del Ej4 |
| Ej7 | `ej7` | Perfil de los datos y exploración de la API de Yahoo |
| Ej8 | `ej8` | Silver del sentimiento |
| Ej9 | `ej9` | Silver de los precios y dimensión de empresas (bronze y silver) |
| Ej10 | `ej10` | Capa gold: hecho y dimensiones |
| Ej11 | `ej11` | Anti-joins: cotizaciones sin sentimiento y sentimiento sin cotizaciones |
| Ej12 | `ej12` | Columnas calculadas en el hecho |
| Ej13 | `ej13` | Exportación de gold a Parquet |

## Decisiones que se apartan del enunciado
- **`Stock_Splits`** en lugar de `Stock Splits`: Parquet y Delta no admiten espacios en los nombres de columna.
- **`Volume` como `long`**, para evitar desbordamientos.
- **Sin `Adj Close`**, que no está en la lista de columnas del enunciado.
- **`Date` en bronze y `Fecha` desde silver**: bronze es copia fiel de Yahoo; silver unifica la clave con el sentimiento.
- **Fin de descarga exclusivo**: en yfinance el parámetro `end` no se incluye; se usa 2026-10-02 para llegar al 1 de octubre.
- **Sectores**: el CSV ya viene en inglés, con los mismos nombres que Yahoo, así que no se traducen para cruzar;
  `dim_empresa` añade `Sector_ES` para el dashboard.
- **`unionByName`** en lugar de `union`, y `cache()` de la descarga antes del MERGE para no recalcular la fuente.
- **Spark 4 activa el modo ANSI**: los casts inválidos dan error, por eso se usa `try_cast`.

## Tickers
35 valores del IBEX-35, composición vigente desde el 9/07/2024 (Puig sustituyó a Meliá), por lo que cubre todo el
periodo descargado.

## Git
Los ejercicios están en commits separados, con un tag por clase (`clase1`, `clase2`, `clase3`). `data/lake/` y
`data/export/` no se versionan: se regeneran ejecutando el proyecto.
