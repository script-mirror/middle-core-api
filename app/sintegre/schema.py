from pydantic import BaseModel
from typing import List, Optional

class ProductUpdate(BaseModel):
    nome: str
    filename: str