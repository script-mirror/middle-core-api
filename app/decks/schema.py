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