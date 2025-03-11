# API Middle de Comercialização de Energia Raízen

## Estrutura do Projeto

Este projeto está organizado em diversos módulos:

- `app/`: Diretório principal da aplicação
  - `controllers/`: Manipuladores de requisições HTTP
  - `core/`: Configurações e utilitários principais
  - `schemas/`: Modelos Pydantic para validação de requisições/respostas
  - `services/`: Implementação da lógica de negócios
  - `models/`: Modelos de banco de dados e implementações SQLAlchemy Core

## Acesso ao Banco de Dados com SQLAlchemy Core (wx_dbClass)

O projeto utiliza SQLAlchemy Core através da `wx_dbClass` para operações de banco de dados. Exemplo de uso:

```python
from wx_dbClass import wx_dbClass

class MeuServico:
    def __init__(self):
        self.db = wx_dbClass()
    
    def obter_dados(self):
        query = "SELECT * FROM minha_tabela"
        resultado = self.db.execute(query)
        return resultado
```

## Exemplo de Organização de Módulo (app/rodadas/)

### 1. Schemas
Localizados em `app/rodadas/schemas.py`, definem validação e serialização de dados:
```python
from pydantic import BaseModel

class RodadaBase(BaseModel):
    nome: str
    data: datetime
```

### 2. Serviços
Localizados em `app/rodadas/service.py`, implementam a lógica de negócios:
```python
class RodadaService:
    def __init__(self):
        self.db = wx_dbClass()
    
    def criar_rodada(self, rodada: RodadaBase):
        # Implementação da lógica de negócios
        pass
```

### 3. Controllers
Localizados em `app/rodadas/controller.py`, manipulam requisições HTTP:
```python
@router.post("/rodadas/")
async def criar_rodada(rodada: RodadaBase):
    service = RodadaService()
    return service.criar_rodada(rodada)
```