from enum import Enum

class ModelosChuvaEstacaoChuvosa(str, Enum):
    gfs = 'gfs'
    gefs = 'gefs'

class RegioesChuvaEstacaoChuvosa(str, Enum):
    sudeste = 'sudeste'
    norte = 'norte'