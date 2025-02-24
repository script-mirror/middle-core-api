from pydantic import BaseModel
import datetime

class ChuvaPrevisaoResposta(BaseModel):
    modelo: str
    dt_rodada: datetime.date
    hr_rodada: int
    id: int
    dt_prevista: datetime.date
    vl_chuva: float
    dia_semana: str
    semana: int

# "modelo": "GFS",
#     "dt_rodada": "2024-08-26",
#     "hr_rodada": 6,
#     "id": 1,
#     "dt_prevista": "2024-08-27",
#     "vl_chuva": 0.18,
#     "dia_semana": "Tuesday",
#     "semana": 1