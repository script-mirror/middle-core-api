from fastapi import APIRouter
from . import service
from .schema import *
import datetime
from typing import Optional

##############################################################################################################################################################

router = APIRouter(prefix='/meteorologia')

##############################################################################################################################################################

@router.get('/estacao-chuvosa',tags=['Meteorologia'])
def get_chuva_cpc_estacao_chuvosa(regiao:RegioesChuvaEstacaoChuvosa, dt_ini_obs: Optional[datetime.date] = None, dt_fim_obs: Optional[datetime.date] = None):

    return service.EstacaoChuvosaObservada.get_chuva_observada(dt_ini_obs, dt_fim_obs, regiao.name) # service.get_chuva_observ.get_bacias(divisao.name)

##############################################################################################################################################################

@router.get('/estacao-chuvosa-prev',tags=['Meteorologia'])
def get_chuva_cpc_estacao_chuvosa_previsao(regiao:RegioesChuvaEstacaoChuvosa, modelo=None, dt_rodada: Optional[datetime.date]=datetime.datetime.now().strftime('%Y-%m-%d'), hr_rodada='00'):

    return service.EstacaoChuvosaObservada.get_chuva_prevista_estacao_chuvosa(dt_rodada=dt_rodada, hr_rodada=hr_rodada, regiao=regiao.name, modelo=modelo) # service.get_chuva_observ.get_bacias(divisao.name)

##############################################################################################################################################################

@router.get('/climatologia-bacias', tags=['Meteorologia'])
def get_climatologia_bacias():

    return service.ClimatologiaChuva.get_climatologia()

##############################################################################################################################################################