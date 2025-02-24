from pydantic import BaseModel
import datetime

class RodadaCriacao(BaseModel):
    dt_rodada: datetime.date
    hr_rodada: int
    str_modelo: str
    fl_estudo: bool