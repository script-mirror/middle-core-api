from sys import path

from fastapi import APIRouter
from typing import List, Optional
from . import service
from .schema import *
import datetime


router = APIRouter(prefix="/decks")


@router.post("/weol", tags=["Decomp"])
def post_weol(
    body: List[WeolSemanalSchema]
):
    return service.WeolSemanal.create(body)


@router.get("/weol", tags=["Decomp"])
def get_weol(
    data_produto: datetime.date
):
    return service.WeolSemanal.get_by_product_date(data_produto)


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


@router.get("/patamares/weighted-average", tags=["Decomp"])
def get_weighted_avg_by_product_date(
    dataProduto: datetime.date,
):
    return service.WeolSemanal.get_weighted_avg_by_product_date(dataProduto)


@router.get("/patamares/weighted-average/month/table", tags=["Decomp"])
def get_weighted_avg_table_monthly_by_product_date(
    dataProduto: datetime.date,
    quantidadeProdutos: int
):
    return service.WeolSemanal.get_weighted_avg_table_monthly_by_product_date(dataProduto, quantidadeProdutos)


@router.get("/patamares/weighted-average/week/table", tags=["Decomp"])
def get_weighted_avg_table_weekly_by_product_date(
    dataProduto: datetime.date,
    quantidadeProdutos: int
):
    return service.WeolSemanal.get_weighted_avg_table_weekly_by_product_date(dataProduto, quantidadeProdutos)


@router.get("/cvu/usinas", tags=["CVU"])
def get_usinas_cvu():
    return service.CvuUsinasTermicas.get_usinas_termicas()


@router.get("/cvu", tags=["CVU"])
def get_usinas_cvu(
    ano_mes_referencia: Optional[datetime.date] = None,
    dt_atualizacao: Optional[datetime.datetime] = None,
    fonte: Optional[str] = None,
):
    return service.Cvu.get_cvu_by_params_deck(
        ano_mes_referencia=ano_mes_referencia,
        dt_atualizacao=dt_atualizacao,
        fonte=fonte
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


@router.get("/carga-patamar", tags=["Decomp"])
def get_carga_decomp_by_date(
    dataProduto: datetime.date
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
def get_check_cvu_by_data_atualizacao_title(
    data_atualizacao: datetime.datetime,
    title: str
):
    return service.CheckCvu.get_by_data_atualizacao_title(data_atualizacao, title)
