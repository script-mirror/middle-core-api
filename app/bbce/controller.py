from sys import path

from fastapi import APIRouter
from typing import List, Optional
from . import service
from app.core.utils import cache
from .schema import *
import datetime


router = APIRouter(prefix="/bbce")


@router.get("/produtos-interesse", tags=["BBCE"])
def get_produtos_interesse():
    return service.ProdutoInteresse.get_all()


@router.post("/produtos-interesse", tags=["BBCE"])
def post_produtos_interesse(
        produtos_interesse: List[ProdutoInteresseSchema]
):
    return service.ProdutoInteresse.post(produtos_interesse)


@router.get("/produtos-interesse/html", tags=["BBCE"])
def get_produtos_interesse(
    data: datetime.date,
    tipo_negociacao: Optional[CategoriaNegociacaoEnum] = None
):
    return service.NegociacoesResumo.get_html_table_negociacoes_bbce(data, tipo_negociacao)


@router.get("/negociacoes/categorias", tags=["BBCE"])
def get_categorias_negociacoes():
    return service.CategoriaNegociacao.get_all()


@router.get('/resumos-negociacoes', tags=['BBCE'])
def get_negociacoes_bbce(
    produto: str,
    categoria_negociacao: Optional[CategoriaNegociacaoEnum] = None,
    no_cache: bool = True,
    atualizar: bool = False
):
    if no_cache:
        return service.NegociacoesResumo.get_negociacao_bbce(produto, categoria_negociacao)
    return cache.get_cached(service.NegociacoesResumo.get_negociacao_bbce, produto, categoria_negociacao, atualizar=atualizar)


@router.get('/resumos-negociacoes/negociacoes-de-interesse', tags=['BBCE'])
def get_negociacoes_interesse_bbce_por_data(
    datas: datetime.date,
    categoria_negociacao: Optional[CategoriaNegociacaoEnum] = None,
    no_cache: bool = True,
    atualizar: bool = False
):

    if no_cache:
        return service.NegociacoesResumo.get_negociacoes_interesse_por_data(datas, categoria_negociacao)
    return cache.get_cached(service.NegociacoesResumo.get_negociacoes_interesse_por_data, datas, categoria_negociacao, atualizar=atualizar)


@router.get('/resumos-negociacoes/negociacoes-de-interesse/fechamento', tags=['BBCE'])
def get_negociacoes_fechamento_interesse_por_data(
    datas: datetime.date,
    categoria_negociacao: Optional[CategoriaNegociacaoEnum] = None,
    no_cache: bool = True,
    atualizar: bool = False
):

    if no_cache:
        return service.NegociacoesResumo.get_negociacoes_fechamento_interesse_por_data(datas, categoria_negociacao)
    return cache.get_cached(service.NegociacoesResumo.get_negociacoes_fechamento_interesse_por_data, datas, categoria_negociacao, atualizar=atualizar)


@router.get('/resumos-negociacoes/negociacoes-de-interesse/ultima-atualizacao', tags=['BBCE'])
def get_datahora_ultima_negociacao(
    categoria_negociacao: Optional[CategoriaNegociacaoEnum] = None,
    no_cache: bool = True,
    atualizar: bool = False
):
    if no_cache:
        return service.Negociacoes.get_datahora_ultima_negociacao(categoria_negociacao)
    return cache.get_cached(service.Negociacoes.get_datahora_ultima_negociacao, categoria_negociacao, atualizar=atualizar)


@router.get('/resumos-negociacoes/spread/preco-medio', tags=['BBCE'])
def get_spread_preco_medio_negociacoes(
    produto1: str,
    produto2: str,
    categoria_negociacao: Optional[CategoriaNegociacaoEnum] = None,
    no_cache: bool = True,
    atualizar: bool = False
):

    if no_cache:
        return service.NegociacoesResumo.spread_preco_medio_negociacoes(produto1, produto2, categoria_negociacao)
    return cache.get_cached(service.NegociacoesResumo.spread_preco_medio_negociacoes, produto1, produto2, categoria_negociacao, atualizar=atualizar)
