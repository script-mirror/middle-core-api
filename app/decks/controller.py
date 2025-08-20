from fastapi import APIRouter
from typing import List, Optional
from . import service
from .schema import (
    RestricoesEletricasSchema,
    CargaNewaveSistemaEnergiaCreateDto,
    CargaNewaveSistemaEnergiaUpdateDto,
    CargaNewaveCadicCreateDto,
    CargaNewaveCadicUpdateDto,
    CvuSchema,
    CvuMerchantSchema,
    WeolSemanalSchema,
    PatamaresDecompSchema,
    CargaSemanalDecompSchema,
    CargaPmoSchema,
    NewavePatamarCargaUsinaSchema,
    NewavePatamarIntercambioSchema,
    CheckCvuCreateDto,
    TipoCvuEnum,
    IndiceBlocoEnum,
    SubmercadosEnum,
    PatamaresEnum,
    NewavePrevisoesCargasReadDto,
    )
import datetime


router = APIRouter(prefix="/decks")


@router.post("/weol", tags=["Decomp"])
def post_weol(
    body: List[WeolSemanalSchema]
):
    return service.WeolSemanal.create(body)

@router.get("/weol", tags=["Decomp"])
def get_weol(
    data_produto: Optional[datetime.date] = None
):
    return service.WeolSemanal.get_by_product_date(data_produto)

@router.get("/weol/last-deck-date", tags=["Decomp"])
def get_last_deck_date_weol():
    """
    Retorna a data do último deck de WEOL disponível.
    """
    return service.WeolSemanal.get_last_deck_date_weol()

@router.get("/weol/product-date", tags=["Decomp"])
def get_by_product_date_start_week_year_month_rv(
    dataProduto: datetime.date,
    mesEletrico: int,
    ano: int,
    rv: int
):
    return service.WeolSemanal.get_by_product_date_start_week_year_month_rv(dataProduto, mesEletrico, ano, rv)


@router.get("/weol/start-week-date", tags=["Decomp"])
def get_weol_by_product_date_start_week(
    inicioSemana: datetime.date,
    dataProduto: datetime.date
):
    return service.WeolSemanal.get_by_product_start_week_date_product_date(inicioSemana, dataProduto)


@router.delete("/weol", tags=["Decomp"])
def delete_weol(
    data_produto: datetime.date
):
    return service.WeolSemanal.delete_by_product_date(data_produto)


@router.post("/patamares", tags=["Decomp"])
def post_patamares(
    body: List[PatamaresDecompSchema]

):
    return service.Patamares.create(body)


@router.delete("/patamares", tags=["Decomp"])
def delete_patamares(
    data_inicio: datetime.date
):
    return service.Patamares.delete_by_start_date(data_inicio)


@router.get("/patamares", tags=["Decomp"])
def get_weol_by_product_date_start_week_year_month_rv(
    inicioSemana: datetime.date,
    fimSemana: datetime.date
):
    return service.Patamares.get_horas_por_patamar_por_inicio_semana_data(inicioSemana, fimSemana)


@router.get("/weol/weighted-average", tags=["Decomp"])
def get_weighted_avg_by_product_date(
    dataProduto: datetime.date,
):
    return service.WeolSemanal.get_weighted_avg_by_product_date(dataProduto)


@router.get("/weol/diff-table", tags=["Decomp"])
def get_weol_day_minus_one_diff(
    dataProduto: datetime.date,
    dias_para_subtrair: int = 1
):
    return service.WeolSemanal.get_weol_day_minus_one_diff(
        dataProduto, dias_para_subtrair
    )


@router.get("/weol/weighted-average/month/table", tags=["Decomp"])
def get_weighted_avg_table_monthly_by_product_date(
    dataProduto: datetime.date,
    quantidadeProdutos: int
):
    return service.WeolSemanal.get_weighted_avg_table_monthly_by_product_date(dataProduto, quantidadeProdutos)


@router.get("/weol/weighted-average/week/table", tags=["Decomp"])
def get_weighted_avg_table_weekly_by_product_date(
    dataProduto: datetime.date,
    quantidadeProdutos: int
):
    return service.WeolSemanal.get_weighted_avg_table_weekly_by_product_date(dataProduto, quantidadeProdutos)


@router.get("/cvu/usinas", tags=["CVU"])
def get_usinas_cvu():
    return service.CvuUsinasTermicas.get_usinas_termicas()


@router.get("/cvu", tags=["CVU"])
def get_cvu_by_tipo_data_atualizacao(
    dt_atualizacao: Optional[datetime.datetime] = None,
    fonte: Optional[str] = None,
):
    return service.Cvu.get_cvu_by_tipo_data_atualizacao(
        data_atualizacao=dt_atualizacao,
        tipo_cvu=fonte
    )


@router.post("/cvu", tags=["CVU"])
def post_cvu(
    body: List[CvuSchema]
):
    return service.Cvu.create(body)


@router.post("/cvu/merchant", tags=["CVU"])
def post_cvu_merchant(
    body: List[CvuMerchantSchema]
):
    return service.CvuMerchant.create(body)


@router.post("/carga-decomp", tags=["Decomp"])
def post_carga_decomp(
    body: List[CargaSemanalDecompSchema]
):
    return service.CargaSemanalDecomp.create(body)


@router.get("/carga-decomp", tags=["Decomp"])
def get_carga_decomp_by_date(
    dataProduto: datetime.date | None = None
):
    return service.CargaSemanalDecomp.get_by_product_date(dataProduto)


@router.post("/carga-pmo", tags=["PMO"])
def post_carga_pmo(
    body: List[CargaPmoSchema]
):
    return service.CargaPmo.create(body)


@router.get("/carga-pmo", tags=["PMO"])
def get_carga_pmo():
    return service.CargaPmo.get_most_recent_data()


@router.get("/carga-pmo/historico-previsao", tags=["PMO"])
def get_carga_pmo_historico_previsao(
    dt_referencia: Optional[datetime.date] = None,
    revisao: Optional[str] = None
):
    """
    Retorna os dados de carga PMO separados em histórico (realizados) e previsões.

    Args:
        dt_referencia: Data de referência para comparação (se não fornecida, usa a data atual)
        revisao: Número da revisão a ser considerada

    Returns:
        Um dicionário com dados históricos e previsões separados
    """
    # Se não for fornecida data de referência, usa a data atual
    if dt_referencia is None:
        dt_referencia = datetime.date.today()

    # Se não for fornecida revisão, usa a mais recente
    if revisao is None:
        # Implementação para obter a revisão mais recente
        # (temporariamente usando "4" como exemplo)
        revisao = "0"

    return service.CargaPmo.get_historico_versus_previsao(dt_referencia, revisao)

@router.get("/newave/previsoes_cargas", tags=["Newave"])
def get_newave_previsoes_cargas(
    dt_referente: Optional[datetime.date] = None,
    submercado: Optional[SubmercadosEnum] = None,
    patamar: Optional[PatamaresEnum] = None
) -> List[NewavePrevisoesCargasReadDto]:
    """
    Retorna as previsões de cargas do Newave.
    
    Parameters:
    - dt_referente: Data de referência para filtrar os resultados
    - submercado: Código do submercado para filtrar os resultados
    - patamar: Patamar para filtrar os resultados
    
    Returns:
    - Lista de registros de previsões de cargas
    """
    return service.NewavePrevisoesCargas.get_previsoes_cargas(dt_referente, submercado, patamar)


@router.post("/newave/sistema", tags=["Newave"])
def post_newave_sist_energia(
    body: List[CargaNewaveSistemaEnergiaCreateDto]
):
    return service.NewaveSistEnergia.post_newave_sist_energia(body)

@router.get("/newave/sistema/total_unsi", tags=["Newave"])
def get_sist_total_unsi_deck_values():
    return service.NewaveSistEnergia.get_sist_total_unsi_deck_values()

@router.put("/newave/sistema/mmgd_total", tags=["Newave"])
def put_sist_mmgd_total_com_previsoes_cargas_mensais(
    body: List[CargaNewaveSistemaEnergiaUpdateDto]
):
    return service.NewaveSistEnergia.put_sist_mmgd_com_previsoes_cargas_mensais(body)

@router.get("/newave/sistema/cargas/total_carga_global", tags=["Newave"])
def get_sist_total_carga_global_deck_values():
    return service.NewaveSistEnergia.get_sist_total_carga_global_deck_values()

@router.get("/newave/sistema/cargas/total_carga_liquida", tags=["Newave"])
def get_sist_total_carga_liquida_deck_values():
    return service.NewaveSistEnergia.get_sist_total_carga_liquida_deck_values()

@router.get("/newave/sistema/mmgd_total", tags=["Newave"])
def get_sist_mmgd_total_deck_values():
    """
    Retorna os valores totais de MMGD (soma de MMGD base e MMGD expansão)
    para os dois decks mais recentes.
    
    Returns:
        Lista com informações dos decks, contendo dados agregados de MMGD total
        (MMGD base + MMGD expansão), organizados por mês e ano.
    """
    return service.NewaveSistEnergia.get_sist_mmgd_total_deck_values()


@router.post("/newave/cadic", tags=["Newave"])
def post_newave_cadic(
    body: List[CargaNewaveCadicCreateDto]
):
    return service.NewaveCadic.post_newave_cadic(body)

@router.get("/newave/cadic/total_mmgd_base", tags=["Newave"])
def get_cadic_total_mmgd_base_deck_values():
    return service.NewaveCadic.get_cadic_total_mmgd_base_deck_values()

@router.put("/newave/cadic/total_mmgd_base", tags=["Newave"])
def put_cadic_total_mmgd_base_deck_values(
    body: List[CargaNewaveCadicUpdateDto]
):
    return service.NewaveCadic.put_cadic_total_mmgd_base_deck_values(body)

@router.get("/newave/cadic/total_ande", tags=["Newave"])
def get_cadic_total_ande_deck_values():
    return service.NewaveCadic.get_cadic_total_ande_deck_values()


@router.post("/newave/patamar/carga_usinas", tags=["Newave"])
def post_newave_patamar_carga_usinas(
    body: List[NewavePatamarCargaUsinaSchema]
):
    return service.NewavePatamarCargaUsina.post_newave_patamar_carga_usina(body)

@router.get("/newave/patamar/carga_usinas/dt_entre", tags=["Newave"])
def get_newave_patamar_carga_usinas_by_dt_referente(
    dt_inicial: datetime.date,
    dt_final: datetime.date,
    indice_bloco: Optional[IndiceBlocoEnum] = None,
):
    return service.NewavePatamarCargaUsina.get_patamar_carga_by_dt_referente(dt_inicial, dt_final, indice_bloco)

@router.post("/newave/patamar/intercambio", tags=["Newave"])
def post_newave_patamar_intercambio(
    body: List[NewavePatamarIntercambioSchema]
):
    return service.NewavePatamarIntercambio.post_newave_patamar_intercambio(body)

@router.get("/newave/patamar/intercambio/dt_entre", tags=["Newave"])
def get_newave_patamar_intercambio_by_dt_referente(
    dt_inicial: datetime.date,
    dt_final: datetime.date
):
    return service.NewavePatamarIntercambio.get_patamar_intercambio_by_dt_referente(dt_inicial, dt_final)







@router.post("/check-cvu", tags=["CVU"])
def post_check_cvu(
    body: CheckCvuCreateDto
):
    return service.CheckCvu.create(body)


@router.get("/check-cvu/{check_cvu_id}", tags=["CVU"])
def get_check_cvu_by_id(
    check_cvu_id: int
):
    return service.CheckCvu.get_by_id(check_cvu_id)


@router.patch("/check-cvu/{check_cvu_id}/status", tags=["CVU"])
def update_check_cvu_status(
    check_cvu_id: int,
    status: str
):
    return service.CheckCvu.update_status_by_id(check_cvu_id, status)


@router.get("/check-cvu", tags=["CVU"])
def get_check_cvu_by_data_atualizacao_tipo_cvu(
    data_atualizacao: datetime.datetime,
    tipo_cvu: TipoCvuEnum
):
    return service.CheckCvu.get_by_data_atualizacao_tipo_cvu(
        data_atualizacao,
        tipo_cvu
    )


@router.get("/historico-cvu", tags=["CVU"])
def get_all(
        page: int = 1,
        page_size: int = 40
):
    return service.CheckCvu.get_all(page, page_size)


@router.get("/dessem/previsao", tags=["Dessem"])
def get_previsao_dessem():
    """
    Obtém a previsão DESSEM (IPDO):
    - Deck mais recente completo
    - Se faltarem registros de hoje, busca somente hoje no deck anterior
    - Agrega por dia e sigla, retornando dict aninhado
    """
    return service.DessemPrevisao.get_previsao_dessem()


@router.post("/restricoes-eletricas", tags=["Restricoes Eletricas"])
def create_restricoes_eletricas(
    body: List[RestricoesEletricasSchema]
):
    """
    Cria novas restrições elétricas.
    """
    return service.RestricoesEletricas.post_restricoes_eletricas(body)

@router.get("/restricoes-eletricas", tags=["Restricoes Eletricas"])
def get_restricoes_eletricas_by_data_produto(
    data_produto: Optional[datetime.date] = None,
):
    """
    Obtém restrições elétricas por data de produto.
    """
    return service.RestricoesEletricas.get_restricoes_eletricas_by_data_produto(data_produto)

@router.get("/restricoes-eletricas/historico", tags=["Restricoes Eletricas"])
def get_restricoes_eletricas_historico():
    return service.RestricoesEletricas.get_datas_produto()