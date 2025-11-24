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

@router.post('/estacao-chuvosa', tags=['Meteorologia'])
def post_chuva_cpc_estacao_chuvosa(body: List[ChuvaObservadaCreateDTO]):
    """
        Insere os dados de chuva prevista para a estação chuvosa.
        
            - regiao (str): Regiao especifica da chuva. Regioes aceitos:
                * norte
                * sudeste
                
            - dt_* (str): Datas no formato 'YYYY-MM-DD'
                
    """
    return service.EstacaoChuvosaObservada.post_chuva_observada(body)

##############################################################################################################################################################


@router.get('/estacao-chuvosa-prev', tags=['Meteorologia'])
def get_chuva_cpc_estacao_chuvosa_previsao(regiao: RegioesChuvaEstacaoChuvosa, modelo=None, dt_rodada: Optional[datetime.date] = datetime.datetime.now().strftime('%Y-%m-%d'), hr_rodada='00'):
    # service.get_chuva_observ.get_bacias(divisao.name)
    return service.EstacaoChuvosaPrevista.get_chuva_prevista_estacao_chuvosa(dt_rodada=dt_rodada, hr_rodada=hr_rodada, regiao=regiao.name, modelo=modelo)

@router.post('/estacao-chuvosa-prev', tags=['Meteorologia'])
def post_chuva_cpc_estacao_chuvosa_previsao(body: List[ChuvaPrevistaCreateDTO]):
    """
        Insere os dados de chuva prevista para a estação chuvosa.
        
            - str_modelo (str): Nome do modelo utilizado. Modelos aceitos:
                * gfs
                * gefs
                * gefs-estendido
                * ecmwf
                * ecmwf-aifs
                * ecmwf-ens
                * ecmwf-ens-estendido
               
            - regiao (str): Regiao especifica da chuva. Regioes aceitos:
                * norte
                * sudeste
                
            - dt_* (str): Datas no formato 'YYYY-MM-DD'
                
    """
    return service.EstacaoChuvosaPrevista.post_chuva_prevista_estacao_chuvosa(body)

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

@router.post('/indices-sst-previstos', tags=['Meteorologia'])
def post_indices_sst_previstos(df_tc: List[dict]):
    """
    Cria cadastro de índices SST previstos.
    """

    return service.IndicesSST.insert_indices_sst_previsto(df_tc)

##############################################################################################################################################################

@router.post('/estacoes-meteorologicas', tags=['Meteorologia'])
def post_estacoes_chuva(request: List[EstacoesMeteorologicasPostRequest]):
    """
    Cria cadastro dos dados de chuva das estações.
    """

    service.EstacoesMeteorologicas.insert_dados_estacao(request)

    return {
        "message": "Cadastro de estações de chuva criado com sucesso.",
    }

##############################################################################################################################################################

@router.get('/infos-estacoes-meteorologicas', tags=['Meteorologia'])
def get_infos_estacoes_meteorologicas():
    """
    Obtém informações das estações meteorológicas.
    """
    return service.EstacoesMeteorologicas.get_infos_estacao()

##############################################################################################################################################################

@router.post('/indices-itcz-observados', tags=['Meteorologia'])
def post_indices_itcz_observados(body: List[IndicesITCZObservadosCreateDTO]):
    """
    
    """
    return service.IndicesITCZObservados.post_indices_itcz_observados(body)

@router.get('/indices-itcz-observados', tags=['Meteorologia'])
def get_indices_itcz_observados(dt_inicio: datetime.date = None, dt_fim: datetime.date = None) -> List[IndicesITCZObservadosReadDTO]:
    """
    
    """
    return service.IndicesITCZObservados.get_indices_itcz_observados(dt_inicio, dt_fim)


@router.post('/indices-itcz-previstos', tags=['Meteorologia'])
def post_indices_itcz_previstos(body: List[IndicesITCZPrevistosCreateDTO]):
    """
    
    """
    return service.IndicesITCZPrevistos.post_indices_itcz_previstos(body)

@router.get('/indices-itcz-previstos', tags=['Meteorologia'])
def get_indices_itcz_previstos(dt_inicio: datetime.date = None, dt_fim: datetime.date = None) -> List[IndicesITCZPrevistosCreateDTO]:
    """
    
    """
    return service.IndicesITCZPrevistos.get_indices_itcz_previstos(dt_inicio, dt_fim)
    