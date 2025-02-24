from pydantic import BaseModel
import datetime

class RodadaSmap(BaseModel):
    dt_rodada: datetime.date
    hr_rodada: int
    str_modelo: str