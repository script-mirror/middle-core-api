from sys import path
from dataclasses import dataclass


from app.schemas.chuvaprevisao.ChuvaPrevisaoCriacao import ChuvaPrevisaoCriacao
from app.schemas.chuvaprevisao.ChuvaPrevisaoResposta import ChuvaPrevisaoResposta

@dataclass
class ChuvaPrevisaoMapper():
    def resposta_para_criacao(resposta: ChuvaPrevisaoResposta):
        criacao = ChuvaPrevisaoCriacao()
        criacao.cd_subbacia = resposta.id
        criacao.dt_prevista = resposta.dt_prevista
        criacao.vl_chuva = resposta.vl_chuva
        
    pass