from fastapi import APIRouter, Depends, HTTPException
from typing import List
import datetime

from app.bot_sintegre.service import ProductService

router = APIRouter(prefix="/bot-sintegre", tags=["Bot Sintegre"])
product_service = ProductService()

@router.post("/trigger")
def trigger_products(productDate:datetime.date):
    return product_service.trigger(productDate)

@router.post("/trigger/{product_id}")
def trigger_by_id(product_id: int, productDate:datetime.date):
    return product_service.trigger_by_id(productDate,product_id)

@router.get("/{product_id}")
def get_product_by_id(product_id: int):
    product = product_service.get_product_by_id(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Produto nao encontrado")
    return product

@router.get("")
def get_products():
    return product_service.get_products()