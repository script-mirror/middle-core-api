from pydantic_settings import BaseSettings
from typing import Any, Optional
from fastapi_cognito import CognitoAuth, CognitoSettings, CognitoToken
from app.core.config import settings


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


class CognitoTokenSemUsername(CognitoToken):
    username: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.username is None:
            self.username = self.client_id


cognito = CognitoAuth(
    settings=CognitoSettings.from_global_settings(settings),
    userpool_name="us",
    custom_model=CognitoTokenSemUsername,
)
