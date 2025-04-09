import os
import requests as req
import pdb
import datetime
from typing import Optional
from app.core.config import settings
from app.core.utils.logger import logging
from app.core.utils.auth_token import get_access_token

logger = logging.getLogger(__name__)

def send_message(message:str, file:str, dest:str):
    fields = {
        "destinatario": dest,
        "mensagem": message
    }
    files = {
        "arquivo": file
    }
    res = req.post(
        settings.url_message_api,
        data=fields,
        files=files,
        headers={
            "Authorization": f"Bearer {get_access_token()}"
        }
    )
    logger.info(f"Whatsapp status code: {res.status_code}")