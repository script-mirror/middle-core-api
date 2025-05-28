from pydantic import BaseModel
import datetime
from typing import Optional


class PatamaresDecompSchema(BaseModel):
    inicio:datetime.datetime
    patamar:str
    cod_patamar:int
    dia_semana:str
    dia_tipico:str
    tipo_dia:str
    intervalo:int
    dia:int
    semana:int
    mes:int
    
class WeolSemanalSchema(BaseModel):
    inicio_semana:datetime.date
    final_semana:datetime.date
    data_produto:datetime.date
    submercado:str
    patamar:str
    valor:float
    rv_atual:int
    mes_eletrico:int


class CvuMerchantSchema(BaseModel):
    cd_usina: int
    vl_cvu_cf: float
    vl_cvu_scf: float
    mes_referencia: str
    dt_atualizacao: str
    fonte: str


class CvuSchema(BaseModel):
    cd_usina: int
    vl_cvu: float
    tipo_cvu: str
    mes_referencia: str
    ano_horizonte: int
    dt_atualizacao: str
    fonte: str


class CargaSemanalDecompSchema(BaseModel):
    data_produto: datetime.date
    semana_operativa: datetime.date
    patamar: str
    duracao: float
    submercado: str
    carga: float
    base_cgh: float
    base_eol: float
    base_ufv: float
    base_ute: float
    carga_mmgd: float
    exp_cgh: float
    exp_eol: float
    exp_ufv: float
    exp_ute: float
    estagio: int


class CargaPmoSchema(BaseModel):
    carga: float
    mes: int
    revisao: str
    subsistema: str
    semana: Optional[int] = None
    dt_inicio: str
    tipo: str
    periodicidade_inicial: str
    periodicidade_final: str
