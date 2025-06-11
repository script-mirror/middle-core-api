import os
import requests as req
from app.core.config import settings


def get_access_token() -> str:
    response = req.post(
        settings.cognito_url,
        data=settings.cognito_config,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
