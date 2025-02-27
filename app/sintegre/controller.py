from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional
import datetime
from .schema import *  

from app.sintegre.service import ProductService

router = APIRouter(prefix="/bot-sintegre", tags=["Bot Sintegre"])
product_service = ProductService()

@router.post("/trigger")
def trigger_products(productDate:datetime.date, triggerWebhook:Optional[bool]=True):
    return product_service.trigger(productDate, triggerWebhook)

@router.post("/trigger/{product_id}")
def trigger_by_id(product_id: int, productDate:datetime.date):
    return product_service.trigger_by_id(productDate,product_id)

@router.get("/json/{product_id}")
def get_product_by_id(product_id: int):
    product = product_service.get_product_by_id(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Produto nao encontrado")
    return product

@router.get("/json")
def get_products_json():
    return product_service.get_products_json()

@router.get("")
def get_products():
    return product_service.get_products()


@router.put("/{product_id}")
def update_product(product_id: int, last_received: str):
    return product_service.update_product(product_id, last_received)


@router.post("/verify")
def verify_if_is_new(
    product_details:ProductUpdate
    ):
    return product_service.verify_if_is_new(
        product_details
    )