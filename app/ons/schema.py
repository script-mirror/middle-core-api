from enum import Enum
from typing import Optional, List
from pydantic import BaseModel
import datetime


class DivisaoBaciasEnum(str, Enum):
    tb_chuva = 'tb_chuva'
    tb_bacias = 'tb_bacias'
    tb_bacias_segmentadas = 'tb_bacias_segmentadas'


class GranularidadeEnum(str, Enum):
    bacia = "bacia"
    submercado = "submercado"


class EnaAcomphSchema(BaseModel):
    data: datetime.date
    granularidade: str
    localizacao: str
    ena: float


class AcomphSchema(BaseModel):
    dt_referente: datetime.date
    cd_posto: int
    vl_vaz_def_conso: float | None
    vl_vaz_inc_conso: float | None
    vl_vaz_nat_conso: float | None
    dt_acomph: datetime.date

class RdhCreateDto(BaseModel):
    cd_posto: int
    vl_vol_arm_perc: Optional[float] = None
    vl_mlt_vaz: Optional[float] = None
    vl_vaz_dia: Optional[float] = None
    vl_vaz_turb: Optional[float] = None
    vl_vaz_vert: Optional[float] = None
    vl_vaz_dfl: Optional[float] = None
    vl_vaz_transf: Optional[float] = None
    vl_vaz_afl: Optional[float] = None
    vl_vaz_inc: Optional[float] = None
    vl_vaz_consunt: Optional[float] = None
    vl_vaz_evp: Optional[float] = None
    dt_referente: datetime.date