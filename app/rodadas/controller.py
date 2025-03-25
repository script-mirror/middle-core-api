from sys import path


from fastapi import APIRouter
from fastapi.responses import JSONResponse

from . import service
from app.core.utils import cache
from .schema import *


import datetime
from typing import Optional, List

router = APIRouter(prefix='/rodadas')


@router.get('/',tags=['Rodadas'])
def get_rodadas(
    dt:Optional[datetime.date] = None,
    no_cache:Optional[bool] = True,
    atualizar:Optional[bool] = False
    ):
    if no_cache:
        return service.CadastroRodadas.get_rodadas_por_dt(dt)
    return cache.get_cached(service.CadastroRodadas.get_rodadas_por_dt, dt, atualizar=atualizar)

@router.get('/por-id/{idRodada}',tags=['Rodadas'])
def get_rodadas_by_id(
    idRodada:int
    ):
    return service.CadastroRodadas.get_rodadas_by_id(idRodada)

@router.get('/historico',tags=['Rodadas'])
def get_historico_rodadas_por_nome(
    nomeModelo:str
    ):
    return service.CadastroRodadas.get_historico_rodadas_por_nome(nomeModelo)



@router.get('/chuva/previsao', tags=['Rodadas'])
def get_chuva_previsao_por_id_data_entre_granularidade(
    nome_modelo:Optional[str] = None,
    dt_hr_rodada:Optional[datetime.datetime] = None,
    granularidade:Optional[GranularidadeEnum] = GranularidadeEnum.subbacia,
    id_chuva:Optional[int] = None,
    dt_inicio_previsao:Optional[datetime.date] = None,
    dt_fim_previsao:Optional[datetime.date] = None,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    if id_chuva:
        return service.Chuva.get_chuva_por_id_data_entre_granularidade(id_chuva, granularidade.name, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar)
    elif nome_modelo and dt_hr_rodada:
        return service.Chuva.get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo, dt_hr_rodada, granularidade.name, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar)

@router.get('/subbacias', tags=['Rodadas'])
def get_subbacias():
    return service.Subbacia.get_subbacia()

@router.post('/chuva/previsao/pesquisa/{granularidade}', tags=['Rodadas'], response_model=List[ChuvaPrevisaoResposta])
def get_previsao_chuva_modelos_combinados(
    previsao:List[PesquisaPrevisaoChuva],
    granularidade:GranularidadeEnum,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    return service.Chuva.get_previsao_chuva_modelos_combinados(previsao, granularidade.name, no_cache, atualizar)

@router.post('/chuva/previsao/modelos', tags=['Rodadas'])
def post_chuva_modelo_combinados(
    chuva_prev: List[ChuvaPrevisaoCriacao],
    rodar_smap:bool = True,
    prev_estendida:bool = False
):  
    return service.Chuva.post_chuva_modelo_combinados(chuva_prev, rodar_smap, prev_estendida)

@router.post('/chuva/previsao/membros', tags=['Rodadas'])
def post_chuva_membros(
    chuva_prev: List[ChuvaPrevisaoCriacaoMembro],
    rodar_smap:bool = False
):  
    return service.ChuvaMembro.post_chuva_membro(chuva_prev, rodar_smap)

@router.delete('/chuva/previsao', tags=['Rodadas'])
def delete_rodada_chuva_smap_por_id_rodada(
    id_rodada:int
):
    return service.CadastroRodadas.delete_rodada_chuva_smap_por_id_rodada(id_rodada)

@router.get('/chuva/observada', tags=['Rodadas'])
def get_chuva_observada_por_data(
    dt_observada:datetime.date
    ):
    return service.ChuvaObs.get_chuva_observada_por_data(dt_observada)

@router.post('/chuva/observada', tags=['Rodadas'])
def post_chuva_observada(
    chuva_obs:List[ChuvaObsReq]
):
    return service.ChuvaObs.post_chuva_obs(chuva_obs)

@router.get('/chuva/observada/psat', tags=['Rodadas'])
def get_chuva_observada_psat_por_data(
    dt_observada:datetime.date
    ):
    return service.ChuvaObsPsat.get_chuva_observada_psat_por_data(dt_observada)


@router.post('/chuva/observada/psat', tags=['Rodadas'])
def post_chuva_observada_psat(
    chuva_obs:List[ChuvaObsReq]
):
    return service.ChuvaObsPsat.post_chuva_obs_psat(chuva_obs)

@router.post('/smap', tags=['Rodadas'])
def post_smap(
    rodada:RodadaSmap
):  
    return service.Smap.post_rodada_smap(rodada)

@router.get('/smap', tags=['Rodadas'])
def get_vazao_smap_by_id(
    id_smap:int
):
    return service.Smap.get_vazao_smap_by_id(id_smap)

@router.get('/chuva/previsao/membros', tags=['Rodadas'])
def get_chuva_por_nome_modelo_data_entre_granularidade(
    nome_modelo:Optional[str] = None,
    dt_hr_rodada:Optional[datetime.datetime] = None,
    granularidade:Optional[GranularidadeEnum] = GranularidadeEnum.subbacia,
    dt_inicio_previsao:Optional[datetime.date] = None,
    dt_fim_previsao:Optional[datetime.date] = None,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
):
    return service.ChuvaMembro.get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo,dt_hr_rodada,granularidade,dt_inicio_previsao,dt_fim_previsao,no_cache,atualizar)
    
@router.get('/export-rain', tags=['Rodadas'])
def export_rain(id_chuva: int):
    return service.Chuva.export_rain(id_chuva)

@router.get("/export-rain-obs", tags=['Rodadas'])
def chuva_observada_ponderada_submercado(
    data:datetime.date,
    qtd_dias:int
):
    return service.Chuva.get_chuva_observada_ponderada_submercado(data-datetime.timedelta(days=qtd_dias), data)