import os
import requests as r
from app.core.config import settings

def get_access_token() -> str:
    response = r.post(
        settings.cognito_url,
        data=settings.cognito_config,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']