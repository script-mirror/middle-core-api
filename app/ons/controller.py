from sys import path


from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from . import service
from app.core.utils import cache
from .schema import *
import datetime
from typing import Optional

router = APIRouter(prefix='/ons')


@router.get('/bacias',tags=['ONS'])
def get_bacias(
    divisao:DivisaoBaciasEnum,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False):
    if no_cache:
        return service.tb_bacias.get_bacias(divisao.name)
    return cache.get_cached(service.tb_bacias.get_bacias,divisao.name, atualizar=atualizar)

@router.get('/submercados',tags=['ONS'])
def get_submercados(
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False):
    if no_cache:
        return service.tb_submercado.get_submercados()
    return cache.get_cached(service.tb_submercado.get_submercados, atualizar=atualizar)

@router.get('/bacias-segmentadas',tags=['ONS'])
def get_bacias_segmentadas():
     return service.BaciasSegmentadas.get_bacias_segmentadas()

@router.get('/acomph', tags=['ONS'])
def get_acomph_by_dt_referente(
    data:datetime.date
):
    return service.Acomph.get_acomph_by_dt_referente(data)