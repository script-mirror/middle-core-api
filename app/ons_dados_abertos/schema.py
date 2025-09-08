from pydantic import BaseModel
import datetime
from typing import Optional

class DadosHidraulicosUheCreateDto(BaseModel):
    cd_subsistema: Optional[str] = None
    nome_subsistema: Optional[str] = None
    tipo_reservatorio: Optional[str] = None
    nome_bacia: Optional[str] = None
    nome_ree: Optional[str] = None
    cd_reservatorio: Optional[str] = None
    nome_reservatorio: Optional[str] = None
    ordem_cascata: Optional[int] = None
    cd_usina: Optional[int] = None
    data_referente: datetime.date
    nivel_montante: Optional[float] = None
    nivel_jusante: Optional[float] = None
    volume: Optional[float] = None
    vazao_afluente: Optional[float] = None
    vazao_turbinada: Optional[float] = None
    vazao_vertida: Optional[float] = None
    vazao_outras_estruturas: Optional[float] = None
    vazao_defluente: Optional[float] = None
    vazao_transferida: Optional[float] = None
    vazao_natural: Optional[float] = None
    vazao_artificial: Optional[float] = None
    vazao_incremental: Optional[float] = None
    vazao_evaporacao: Optional[float] = None
    vazao_uso_consuntivo: Optional[float] = None
    vazao_incremental_bruta: Optional[float] = None


class DadosHidraulicosSubsistemaCreateDto(BaseModel):
    cd_subsistema: str
    nome_subsistema: str
    data_referente: datetime.date
    ear_max: Optional[float] = None
    ear_mwmes: Optional[float] = None
    ear_percentual: Optional[float] = None
    ena_mwmed: Optional[float] = None
    ena_percentual_mlt: Optional[float] = None
    ena_armazenavel_mwmed: Optional[float] = None
    ena_armazenavel_percentual_mlt: Optional[float] = None

