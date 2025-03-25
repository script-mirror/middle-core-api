from sys import path


from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from . import service
from app.core.utils import cache
from .schema import *
import datetime
from typing import Optional

router = APIRouter(prefix='/pluvia')


@router.get('/bacias',tags=['Pluvia'])
def query_ultimo_id_pluvia_df(data_rodada:datetime.date, flag_pzerada:Optional[bool] = False):
    return service.PluviaEna.query_ultimo_id_pluvia_df(data_rodada, flag_pzerada)