from sys import path


from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.utils import date_util


import datetime
from typing import Optional, List

router = APIRouter(prefix='/utils')


@router.get('/data-eletrica', tags=['Utils'])
def get_data_eletrica(
    data: datetime.date
):
    return date_util.ElecData(data).to_dict()
