from fastapi import APIRouter
from . import service
from .schema import *
import datetime
from typing import Optional

##############################################################################################################################################################

router = APIRouter(prefix='/meteorologia')

##############################################################################################################################################################


@router.get('/estacao-chuvosa', tags=['Meteorologia'])
def get_chuva_cpc_estacao_chuvosa(regiao: RegioesChuvaEstacaoChuvosa, dt_ini_obs: Optional[datetime.date] = None, dt_fim_obs: Optional[datetime.date] = None):
    # service.get_chuva_observ.get_bacias(divisao.name)
    return service.EstacaoChuvosaObservada.get_chuva_observada(dt_ini_obs, dt_fim_obs, regiao.name)

##############################################################################################################################################################


@router.get('/estacao-chuvosa-prev', tags=['Meteorologia'])
def get_chuva_cpc_estacao_chuvosa_previsao(regiao: RegioesChuvaEstacaoChuvosa, modelo=None, dt_rodada: Optional[datetime.date] = datetime.datetime.now().strftime('%Y-%m-%d'), hr_rodada='00'):
    # service.get_chuva_observ.get_bacias(divisao.name)
    return service.EstacaoChuvosaObservada.get_chuva_prevista_estacao_chuvosa(dt_rodada=dt_rodada, hr_rodada=hr_rodada, regiao=regiao.name, modelo=modelo)

##############################################################################################################################################################


@router.get('/climatologia-bacias', tags=['Meteorologia'])
def get_climatologia_bacias():
    return service.ClimatologiaChuva.get_climatologia()

##############################################################################################################################################################


@router.post('/vento-previsto', tags=['Meteorologia'])
def post_rodada_vento_previsto_completo(request: VentoPrevistoRequest):
    """
    Cria cadastro de rodada e insere os valores de vento previsto em uma única operação.
    """
    dt_rodada = request.dt_rodada
    hr_rodada = request.hr_rodada
    modelo = request.modelo
    valores = request.valores

    resultado = service.VentoPrevistoWEOL.insert_vento_previsto(
        valores=valores,
        dt_rodada=dt_rodada,
        hr_rodada=hr_rodada,
        modelo=modelo
    )
    return {
        "message": "Cadastro e valores inseridos com sucesso.",
        "id_cadastro": resultado['id_cadastro'],
        "n_registros_inseridos": resultado['n_registros_inseridos']
    }

##############################################################################################################################################################


@router.get('/vento-previsto', tags=['Meteorologia'])
def get_vento_previsto(
    dt_rodada: Optional[datetime.date] = None,
    hr_rodada: Optional[str] = None,
    modelo: Optional[str] = None,
):
    """
    Obtém os dados de vento previsto para uma rodada específica.
    """
    return service.VentoPrevistoWEOL.get_vento_previsto(
        dt_rodada=dt_rodada,
        hr_rodada=hr_rodada,
        modelo=modelo,
    )

##############################################################################################################################################################

@router.get('/vento-previsto-rodadas', tags=['Meteorologia'])
def get_vento_previsto_rodadas(
    modelo: Optional[str] = None,
    dt_rodada: Optional[datetime.date] = None,
    hr_rodada: Optional[str] = None,
):
    """
    Obtém as rodadas de vento previsto disponíveis.
    """
    return service.VentoPrevistoWEOL.get_rodadas(
        modelo=modelo,
        dt_rodada=dt_rodada,
        hr_rodada=hr_rodada,
    )

##############################################################################################################################################################

@router.get('/indices-diarios-sst', tags=['Meteorologia'])
def get_indices_diarios_sst(
    dt_inicio: Optional[datetime.date] = None,
    dt_fim: Optional[datetime.date] = None,
):
    """
    Obtém os índices diários de SST para um intervalo de datas.
    """
    return service.IndicesSST.get_indices_sst(dt_inicio=dt_inicio, dt_fim=dt_fim)

##############################################################################################################################################################

@router.post('/indices-diarios-sst', tags=['Meteorologia'])
def post_indices_diarios_sst(request: IndicesSSTPOSTRequest):
    """
    Cria cadastro de índices diários de SST.
    """

    service.IndicesSST.insert_indices_sst(request.valores)

    return {
        "message": "Cadastro de índice diário de SST criado com sucesso.",
    }

##############################################################################################################################################################