from pydantic import BaseModel
from datetime import datetime, date

class PatamaresDecompSchema(BaseModel):
    inicio:datetime
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
    inicio_semana:date
    final_semana:date
    data_produto:date
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

