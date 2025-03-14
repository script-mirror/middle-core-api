import uvicorn
from typing import Any, Dict
from fastapi import FastAPI, Depends
from app.core.utils.cache import cache
from app.core.config import settings
from app.core.dependencies import cognito
from fastapi.security import HTTPBearer
from pydantic_settings import BaseSettings
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cognito import CognitoAuth, CognitoSettings, CognitoToken

from app import (
    rodadas_controller,
    ons_controller,
    bbce_controller,
    decks_controller,
    speech_to_text_controller,
    bot_sintegre_controller
)
auth_scheme = HTTPBearer()


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    docs_url=settings.docs_url,
    redoc_url=settings.redoc_url,
    openapi_url=settings.openapi_url
)

origins = [
    "http://localhost:5173",
    "http://localhost:5000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rodadas_controller, prefix="/api/v2")
app.include_router(ons_controller, prefix="/api/v2")
app.include_router(bbce_controller, prefix="/api/v2")
app.include_router(decks_controller, prefix="/api/v2")
app.include_router(speech_to_text_controller, prefix="/api/v2", dependencies=[Depends(auth_scheme), Depends(cognito.auth_required)])
app.include_router(bot_sintegre_controller, prefix="/api/v2")
@app.get("/")
def teste():
    return {"Hello": "World"}

@app.on_event("shutdown")
def shutdown():
    cache.close()
    
def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')
    
if __name__ == "__main__":
    main()