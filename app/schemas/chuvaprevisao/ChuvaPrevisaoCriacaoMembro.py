from pydantic import BaseModel, Field
import datetime

class ChuvaPrevisaoCriacaoMembro(BaseModel):
    cd_subbacia:int = Field(default=1)
    dt_prevista: datetime.date = Field(default=datetime.date.today)
    vl_chuva: float = Field(default=0.0)
    modelo: str = Field(default='TESTE')
    membro: str = Field(default='1')
    dt_hr_rodada: datetime.datetime = Field(default=datetime.datetime.now())
    peso: float = Field(default=1.0)
    
    
    
