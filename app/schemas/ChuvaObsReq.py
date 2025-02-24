from pydantic import BaseModel
import datetime

class ChuvaObsReq(BaseModel):
    cd_subbacia:int
    dt_observado:datetime.date
    vl_chuva:float
