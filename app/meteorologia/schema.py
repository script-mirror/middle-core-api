from enum import Enum
from pydantic import BaseModel
from typing import Dict, List
import datetime


class ModelosChuvaEstacaoChuvosa(str, Enum):
    gfs = 'gfs'
    gefs = 'gefs'
    gefs_estendido = 'gefs-estendido'
    ecmwf = 'ecmwf'
    ecmwf_aifs = 'ecmwf-aifs'
    ecmwf_ens = 'ecmwf-ens'
    ecmwf_ens_estendido = 'ecmwf-ens-estendido'
    


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

class IndiceSST(BaseModel):
    dt_observada: str
    vl_indice: float
    str_indice: str

class IndicesSSTPOSTRequest(BaseModel):
    valores: List[IndiceSST]

class EstacoesMeteorologicas(BaseModel):
    cd_estacao: str
    dt_coleta: str
    vl_chuva: float

class EstacoesMeteorologicasPostRequest(EstacoesMeteorologicas):
    pass

class ChuvaObservadaCreateDTO(BaseModel):
    dt_observada: str
    vl_chuva: float
    regiao: RegioesChuvaEstacaoChuvosa
class ChuvaPrevistaCreateDTO(BaseModel):
    dt_prevista: str
    vl_chuva: float
    str_modelo: ModelosChuvaEstacaoChuvosa
    regiao: RegioesChuvaEstacaoChuvosa
    hr_rodada: int
    dt_rodada: str
    
class IndicesITCZObservadosCreateDTO(BaseModel):
    dt_observada: str
    lats_min: float
    lats_max: float
    lats_menor_olr: float
    lats_menor_vento: float
    lats_media_vento_olr: float
    intensidades_olr: float
    intensidades_chuva: float
    largura: float
    
class IndicesITCZObservadosReadDTO(BaseModel):
    dt_observada: datetime.datetime
    lats_min: float
    lats_max: float
    lats_menor_olr: float
    lats_menor_vento: float
    lats_media_vento_olr: float
    intensidades_olr: float
    intensidades_chuva: float
    largura: float
    
class IndicesITCZPrevistosCreateDTO(BaseModel):
    dt_prevista: str
    dt_rodada: str
    hr_rodada: int
    str_modelo: str
    lats_min: float
    lats_max: float
    lats_menor_olr: float
    lats_menor_vento: float
    lats_media_vento_olr: float
    intensidades_olr: float
    intensidades_chuva: float
    largura: float

    
    
class IndicesITCZPrevistosReadDTO(BaseModel):
    dt_prevista: str
    dt_rodada: str
    hr_rodada: int
    str_modelo: str
    lats_min: float
    lats_max: float
    lats_menor_olr: float
    lats_menor_vento: float
    lats_media_vento_olr: float
    intensidades_olr: float
    intensidades_chuva: float
    largura: float