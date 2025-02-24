from pydantic_settings import BaseSettings
from fastapi_cognito import CognitoAuth, CognitoSettings
from app.core.config import settings
from typing import Any

class Settings(BaseSettings):
    check_expiration: bool = True
    jwt_header_prefix: str = "Bearer"
    jwt_header_name: str = "Authorization"
    userpools: dict[str, dict[str, Any]] = {
        "us": {
            "region": settings.aws_region,
            "userpool_id": settings.cognito_userpool_id,
            "app_client_id": ""
        }
    }


settings = Settings()


cognito = CognitoAuth(
    settings=CognitoSettings.from_global_settings(settings),
    userpool_name="us"
)


