from sys import path

from fastapi import APIRouter
from typing import List, Optional
from . import service
from app.schemas import WeolSemanalSchema, PatamaresDecompSchema
import datetime


router = APIRouter(prefix="/decks")


@router.post("/weol",tags=["Decomp"])
def post_weol(
    body: List[WeolSemanalSchema]
):
    return service.WeolSemanal.create(body)
# @router.get("/weol",tags=["Decomp"])
# def get_weol_all():
#     return service.WeolSemanal.get_all()
@router.get("/weol",tags=["Decomp"])
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

@router.delete("/weol",tags=["Decomp"])
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
    dataProduto:datetime.date,
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