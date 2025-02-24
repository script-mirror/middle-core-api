from pydantic import BaseModel
import datetime
class MembrosModeloSchema(BaseModel):
    id: int
    dt_hr_rodada: datetime.datetime
    nome:str
    id_rodada:int
