import datetime
from fastapi import APIRouter
from typing import List, Optional
from . import service
from .schema import (
    DadosHidraulicosUheCreateDto,
    DadosHidraulicosSubsistemaCreateDto,
    )


router = APIRouter(prefix="/ons-dados-abertos")


@router.post("/dados-hidraulicos-uhe", tags=["Dados Hidraulicos"])
def post_dados_hidraulicos_uhe(
    body: List[DadosHidraulicosUheCreateDto]
):
    return service.DadosHidraulicosUhe.create(body)

@router.get("/dados-hidraulicos-uhe", tags=["Dados Hidraulicos"])
def get_dados_hidraulicos_uhe_by_data_referente(
    data_inicial: datetime.date,
    data_final: Optional[datetime.date] = None
):
    return service.DadosHidraulicosUhe.get_by_data_referente_entre(data_inicial, data_final)

@router.post("/dados-hidraulicos-subsistema", tags=["Dados Hidraulicos"])
def post_dados_hidraulicos_subsistema(
    body: List[DadosHidraulicosSubsistemaCreateDto]
):
    return service.DadosHidraulicosSubsistema.create(body)

@router.get("/dados-hidraulicos-subsistema", tags=["Dados Hidraulicos"])
def get_dados_hidraulicos_subsistema_by_data_referente(
    data_inicial: datetime.date,
    data_final: Optional[datetime.date] = None
):
    return service.DadosHidraulicosSubsistema.get_by_data_referente_entre(data_inicial, data_final) 
