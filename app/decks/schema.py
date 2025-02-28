from pydantic import BaseModel
import datetime

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