from pydantic import BaseModel
import datetime
from typing import Optional


class PatamaresDecompSchema(BaseModel):
    inicio: datetime.datetime
    patamar: str
    cod_patamar: int
    dia_semana: str
    dia_tipico: str
    tipo_dia: str
    intervalo: int
    dia: int
    semana: int
    mes: int


class WeolSemanalSchema(BaseModel):
    inicio_semana: datetime.date
    final_semana: datetime.date
    data_produto: datetime.date
    submercado: str
    patamar: str
    valor: float
    rv_atual: int
    mes_eletrico: int


class CvuMerchantSchema(BaseModel):
    cd_usina: int
    vl_cvu_cf: float
    vl_cvu_scf: float
    mes_referencia: str
    dt_atualizacao: str
    fonte: str
    empreendimento: Optional[str] = None
    despacho: Optional[str] = None
    recuperacao_custo_fixo: Optional[str] = None
    data_inicio: Optional[datetime.date] = None
    data_fim: Optional[datetime.date] = None
    tipo_combustivel: Optional[str] = None
    origem_da_cotacao: Optional[str] = None
    mes_referencia_cotacao: Optional[str] = None


class CvuSchema(BaseModel):
    cd_usina: int
    vl_cvu: float
    tipo_cvu: str
    mes_referencia: str
    ano_horizonte: int
    dt_atualizacao: str
    fonte: str
    agente_vendedor: Optional[str] = None
    tipo_combustivel: Optional[str] = None
    custo_combustivel: Optional[float] = None
    codigo_parcela_usina: Optional[str] = None
    inicio_suprimento: Optional[datetime.date] = None
    termino_suprimento: Optional[datetime.date] = None
    sigla_parcela: Optional[str] = None
    leilao: Optional[str] = None
    cnpj_agente_vendedor: Optional[str] = None
    produto: Optional[str] = None

class CargaSemanalDecompSchema(BaseModel):
    data_produto: datetime.date
    semana_operativa: datetime.date
    patamar: str
    duracao: float
    submercado: str
    carga: float
    base_cgh: float
    base_eol: float
    base_ufv: float
    base_ute: float
    carga_mmgd: float
    exp_cgh: float
    exp_eol: float
    exp_ufv: float
    exp_ute: float
    estagio: int


class CargaPmoSchema(BaseModel):
    carga: float
    mes: int
    revisao: str
    subsistema: str
    semana: Optional[int] = None
    dt_inicio: str
    tipo: str
    periodicidade_inicial: datetime.datetime
    periodicidade_final: datetime.datetime
    
    
class CargaNewaveSistemaEnergiaSchema(BaseModel):
    cd_submercado: int
    vl_ano: int
    vl_mes: int
    vl_energia_total: float
    vl_geracao_pch: float
    vl_geracao_pct: float
    vl_geracao_eol: float
    vl_geracao_ufv: float
    vl_geracao_pch_mmgd: float
    vl_geracao_pct_mmgd: float
    vl_geracao_eol_mmgd: float
    vl_geracao_ufv_mmgd: float
    dt_deck: datetime.datetime
    fonte: str
    

class CargaNewaveCadicSchema(BaseModel):
    vl_ano: int
    vl_mes: int
    vl_const_itaipu: int
    vl_ande: float
    vl_boa_vista: float
    vl_mmgd_se: float
    vl_mmgd_s: float
    vl_mmgd_ne: float
    vl_mmgd_n: float
    dt_deck: datetime.datetime
    fonte: str
        
  

class CheckCvuCreateDto(BaseModel):
    tipo_cvu: Optional[str] = None
    data_atualizacao: Optional[datetime.datetime] = None
    status: Optional[str] = None


class CheckCvuReadDto(BaseModel):
    id: int
    tipo_cvu: Optional[str] = None
    data_atualizacao: Optional[datetime.datetime] = None
    status: Optional[str] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None
