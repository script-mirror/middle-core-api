from pydantic import BaseModel, Field
from enum import Enum
import datetime

class RodadaSmap(BaseModel):
    dt_rodada: datetime.date
    hr_rodada: int
    str_modelo: str
    id_dataviz_chuva: str = Field(default="")
    prev_estendida: bool = Field(default=False)
    
class RodadaCriacao(BaseModel):
    dt_rodada: datetime.date
    hr_rodada: int
    str_modelo: str
    fl_estudo: bool
    
class PesquisaPrevisaoChuva(BaseModel):
    id: int
    dt_inicio: datetime.date
    dt_fim: datetime.date
    
class MembrosModeloSchema(BaseModel):
    id: int
    dt_hr_rodada: datetime.datetime
    nome:str
    id_rodada:int

class ChuvaObsReq(BaseModel):
    cd_subbacia:int
    dt_observado:datetime.date
    vl_chuva:float

class GranularidadeEnum(str, Enum):
    subbacia = 'subbacia'
    bacia = 'bacia'
    submercado = 'submercado'

class TipoRodadaEnum(str, Enum):
    modelo = 'modelo'
    membro = 'membro'


class ChuvaPrevisaoCriacao(BaseModel):
    cd_subbacia:int
    dt_prevista: datetime.date
    vl_chuva: float
    modelo: str
    dt_rodada: datetime.datetime
    
class ChuvaPrevisaoCriacaoMembro(BaseModel):
    cd_subbacia:int = Field(default=1)
    dt_prevista: datetime.date = Field(default=datetime.date.today)
    vl_chuva: float = Field(default=0.0)
    modelo: str = Field(default='TESTE')
    membro: str = Field(default='1')
    dt_hr_rodada: datetime.datetime = Field(default=datetime.datetime.now())
    peso: float = Field(default=1.0)
    
class ChuvaPrevisaoResposta(BaseModel):
    modelo: str
    dt_rodada: datetime.date
    hr_rodada: int
    id: int
    dt_prevista: datetime.date
    vl_chuva: float
    dia_semana: str
    semana: int
