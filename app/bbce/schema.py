from pydantic import BaseModel
from enum import Enum


class ProdutoInteresseSchema(BaseModel):
    str_produto: str
    ordem: int


class CategoriaNegociacaoEnum(str, Enum):
    mesa = 'Mesa'
    boleta_eletronica = 'Boleta Eletronica'
