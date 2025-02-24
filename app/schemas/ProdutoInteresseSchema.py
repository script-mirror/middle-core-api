from pydantic import BaseModel

class ProdutoInteresseSchema(BaseModel):
    str_produto:str
    ordem:int
