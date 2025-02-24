from pydantic import BaseModel
import datetime

class ChuvaPrevisaoCriacao(BaseModel):
    cd_subbacia:int
    dt_prevista: datetime.date
    vl_chuva: float
    modelo: str
    dt_rodada: datetime.datetime
    
