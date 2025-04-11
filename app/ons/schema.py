from enum import Enum
from typing import Optional, List
from pydantic import BaseModel
import datetime

class DivisaoBaciasEnum(str, Enum):
    tb_chuva = 'tb_chuva'
    tb_bacias = 'tb_bacias'
    tb_bacias_segmentadas = 'tb_bacias_segmentadas'
class GranularidadeEnum(str, Enum):
    bacia = "bacia"
    submercado = "submercado"

class EnaAcomphSchema(BaseModel):
    data: datetime.date
    granularidade: str
    localizacao: str
    ena: float