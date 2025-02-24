from pydantic import BaseModel
from datetime import datetime

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
    