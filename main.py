# main.py - punto de entrada
# Uso:
#   python main.py            -> ejecuta todos los ejercicios implementados
#   python main.py ej1a       -> ejecuta solo los ejercicios indicados
import sys

from src.controlador.ibex_controller import IbexController

if __name__ == "__main__":
    IbexController().run(sys.argv[1:] or None)
