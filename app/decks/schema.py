from pydantic import BaseModel
import datetime
from typing import Optional
from enum import Enum


class TipoCvuEnum(str, Enum):
    custo_variavel_unitario_conjuntural = "conjuntural"
    custo_variavel_unitario_estrutural = "estrutural"
    custo_variavel_unitario_conjuntural_revisado = "conjuntural_revisado"
    custo_variavel_unitario_merchant = "merchant"

class PatamaresEnum(str, Enum):
    leve = "leve"
    medio = "medio"
    pesado = "pesado"
    media = "média"
    
class SubmercadosEnum(str, Enum):
    se = "sudeste"
    s = "sul"
    ne = "nordeste"
    n = "norte"
    
class IndiceBlocoEnum(str, Enum):
    CARGA = "CARGA"
    PCH = "PCH"
    PCT = "PCT"
    EOL = "EOL"
    UFV = "UFV"
    PCH_MMGD = "PCH_MMGD"
    PCT_MMGD = "PCT_MMGD"
    EOL_MMGD = "EOL_MMGD"
    UFV_MMGD = "UFV_MMGD"


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
    vl_cvu_cf: Optional[float] = None
    vl_cvu_scf: Optional[float] = None
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
    vl_cvu: Optional[float] = None
    tipo_cvu: Optional[str] = None
    mes_referencia: Optional[str] = None
    ano_horizonte: Optional[int] = None
    dt_atualizacao: Optional[str] = None
    fonte: Optional[str] = None
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

class NewavePrevisoesCargasReadDto(BaseModel):
    data_produto: datetime.date
    data_referente: datetime.date
    submercado: str
    patamar: str
    vl_energia_total: float
    vl_geracao_pch_mmgd: float
    vl_geracao_pct_mmgd: float
    vl_geracao_eol_mmgd: float
    vl_geracao_ufv_mmgd: float

class CargaNewaveSistemaEnergiaCreateDto(BaseModel):
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
    versao: str
    
class CargaNewaveSistemaEnergiaReadDto(BaseModel):
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
    versao: str
    
class CargaNewaveSistemaEnergiaUpdateDto(BaseModel):
    cd_submercado: int
    vl_ano: int
    vl_mes: int
    vl_energia_total: float
    vl_geracao_pch_mmgd: float
    vl_geracao_pct_mmgd: float
    vl_geracao_eol_mmgd: float
    vl_geracao_ufv_mmgd: float
    dt_deck: datetime.datetime
    versao: str


class CargaNewaveCadicCreateDto(BaseModel):
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
    versao: str
    
class CargaNewaveCadicReadDto(BaseModel):
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
    versao: str
    
class CargaNewaveCadicQuadUpdateDto(BaseModel):
    vl_ano: int
    vl_mes: int
    vl_mmgd_se: float
    vl_mmgd_s: float
    vl_mmgd_ne: float
    vl_mmgd_n: float
    vl_boa_vista: Optional[float] = None
    dt_deck: datetime.datetime
    versao: str
    
class CargaNewaveCadicUpdateDto(BaseModel):
    vl_ano: int
    vl_mes: int
    vl_mmgd_se: float
    vl_mmgd_s: float
    vl_mmgd_ne: float
    vl_mmgd_n: float
    dt_deck: datetime.datetime
    versao: str
        
class NewavePatamarCargaUsinaSchema(BaseModel):
    dt_referente: Optional[datetime.date] = None
    patamar: Optional[str] = None
    submercado: Optional[str] = None
    valor_pu: Optional[float] = None
    duracao_mensal: Optional[float] = None
    indice_bloco: Optional[IndiceBlocoEnum] = None
    dt_deck: Optional[datetime.date] = None
    versao: Optional[str] = None


class NewavePatamarIntercambioSchema(BaseModel):
    dt_referente: Optional[datetime.date] = None
    patamar: Optional[str] = None
    submercado_de: Optional[str] = None
    submercado_para: Optional[str] = None
    pu_intercambio_med: Optional[float] = None
    duracao_mensal: Optional[float] = None
    dt_deck: Optional[datetime.date] = None
    versao: Optional[str] = None

class CheckCvuCreateDto(BaseModel):
    tipo_cvu: Optional[TipoCvuEnum] = None
    data_atualizacao: Optional[datetime.datetime] = None
    status: Optional[str] = None


class CheckCvuReadDto(BaseModel):
    id: int
    tipo_cvu: Optional[TipoCvuEnum] = None
    data_atualizacao: Optional[datetime.datetime] = None
    status: Optional[str] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class RestricoesEletricasSchema(BaseModel):
    re: int
    limite: str
    mes_ano: datetime.date
    patamar: str
    valor: float
    data_produto: datetime.date