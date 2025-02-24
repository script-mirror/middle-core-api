from pydantic import BaseModel
import datetime

class PesquisaPrevisaoChuva(BaseModel):
    id: int
    dt_inicio: datetime.date
    dt_fim: datetime.date