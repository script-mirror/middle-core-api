import datetime
from typing import List, Optional

from fastapi import APIRouter

from . import service
from .schema import DivisaoBaciasEnum, EnaAcomphSchema, GranularidadeEnum
from app.core.utils import cache

router = APIRouter(prefix='/ons', tags=['ONS'])


@router.get('/bacias')
def get_bacias(
    divisao: DivisaoBaciasEnum,
    no_cache: Optional[bool] = False,
    atualizar: Optional[bool] = False
):
    if no_cache:
        return service.tb_bacias.get_bacias(divisao.name)
    return cache.get_cached(
        service.tb_bacias.get_bacias, divisao.name, atualizar=atualizar
    )


@router.get('/submercados')
def get_submercados(
    no_cache: Optional[bool] = False,
    atualizar: Optional[bool] = False
):
    if no_cache:
        return service.tb_submercado.get_submercados()
    return cache.get_cached(
        service.tb_submercado.get_submercados, atualizar=atualizar
    )


@router.get('/bacias-segmentadas')
def get_bacias_segmentadas():
    return service.BaciasSegmentadas.get_bacias_segmentadas()


@router.get('/acomph')
def get_acomph_by_dt_referente(
    data: datetime.date
):
    return service.Acomph.get_acomph_by_dt_referente(data)

@router.get('/acomph/data-acomph-entre')
def get_acomphby_dt_acomph(
    data_acomph: datetime.date
):
    service.Acomph.get_acomph_by_dt_acomph(data_acomph)

@router.delete('/ena-acomph/datas')
def delete_ena_acomph(datas: List[datetime.date]):
    return service.EnaAcomph.delete_ena_acomph_by_dates(datas)


@router.post('/ena-acomph')
def post_ena_acomph(ena_acomph: List[EnaAcomphSchema]):
    return service.EnaAcomph.post_ena_acomph(
        [item.dict() for item in ena_acomph]
    )


@router.get('/ena-acomph/entre')
def get_ena_acomph_entre(
    granularidade: GranularidadeEnum,
    data_inicial: datetime.date,
    data_final: datetime.date
):
    return service.EnaAcomph.get_ena_acomph_by_granularidade_date_between(
        granularidade.name, data_inicial, data_final
    )
