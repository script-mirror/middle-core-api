import uvicorn
from fastapi import FastAPI, Depends
from app.core.utils.cache import cache
from app.core.config import settings
from app.core.dependencies import cognito
from fastapi.security import HTTPBearer
from fastapi.middleware.cors import CORSMiddleware

from app import (
    rodadas_controller,
    ons_controller,
    bbce_controller,
    decks_controller,
    meteorologia_controller,
    pluvia_controller,
    utils_controller,
    ons_dados_abertos_controller,
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


controllers_without_auth = [
    rodadas_controller,
    bbce_controller,
]

controllers_with_auth = [
    ons_controller,
    decks_controller,
    meteorologia_controller,
    pluvia_controller,
    ons_dados_abertos_controller,
    utils_controller,
]

for controller in controllers_without_auth:
    app.include_router(controller, prefix="/api/v2")

for controller in controllers_with_auth:
    app.include_router(
        controller, 
        prefix="/api/v2",
        dependencies=[Depends(auth_scheme), Depends(cognito.auth_required)]
    )

@app.get("/api/v2/health")
def health():
    return {"Hello": "World"}


@app.on_event("shutdown")
def shutdown():
    cache.close()


def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')


if __name__ == "__main__":
    main()
