class IbexView:
    """Capa de vista: solo imprime por consola, no calcula ni transforma datos."""

    def titulo(self, ejercicio):
        print(ejercicio)

    def esquema(self, df):
        df.printSchema()

    def filas(self, df, n=5, truncate=True):
        df.show(n, truncate=truncate)

    def valor(self, etiqueta, valor):
        print(f"{etiqueta}: {valor}")

    def mensaje(self, texto):
        print(texto)
