from sys import path


from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from . import service
from app.core.utils import cache
from app.schemas.DivisaoBaciasEnum import DivisaoBaciasEnum
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