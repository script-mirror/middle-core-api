from enum import Enum
from pydantic import BaseModel
from typing import Dict, List


class ModelosChuvaEstacaoChuvosa(str, Enum):
    gfs = 'gfs'
    gefs = 'gefs'


class RegioesChuvaEstacaoChuvosa(str, Enum):
    sudeste = 'sudeste'
    norte = 'norte'


class ValorVento(BaseModel):
    dt_prevista: str
    vl_vento: float
    estado: str
    aglomerado: str


class VentoPrevistoRequest(BaseModel):
    dt_rodada: str
    hr_rodada: int
    modelo: str
    valores: List[ValorVento]
