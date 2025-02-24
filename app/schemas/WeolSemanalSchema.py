from pydantic import BaseModel
from datetime import date

class WeolSemanalSchema(BaseModel):
    inicio_semana:date
    final_semana:date
    data_produto:date
    submercado:str
    patamar:str
    valor:float
    rv_atual:int
    mes_eletrico:int