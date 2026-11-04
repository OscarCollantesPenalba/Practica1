# main.py - punto de entrada de la aplicación
from src.modelo.spark_session import get_spark_session

if __name__ == "__main__":
    # Por ahora solo comprueba que la sesión de Spark con Delta arranca
    spark = get_spark_session()
    print(f"Spark {spark.version} con Delta listo")
    spark.stop()
