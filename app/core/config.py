from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "API Middle"
    version: str = "2.0.0"
    docs_url: str = "/api/v2/docs"
    redoc_url: str = "/api/v2/redoc"
    openapi_url: str = "/api/v2/openapi.json"
    user_airflow:str
    password_airflow: str
    url_airflow_api: str
    host_mysql: str
    port_mysql: str
    user_mysql: str
    password_mysql: str
    cognito_url: str
    cognito_config: str
    sintegre_email: str
    sintegre_password: str
    aws_region:str
    cognito_userpool_id:str
    API_URL:str
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
