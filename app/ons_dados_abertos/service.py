import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
from typing import List
from fastapi import HTTPException
from app.core.utils.logger import logging
from app.core.database.wx_dbClass import db_mysql_master
from .schema import (
    DadosHidraulicosUheCreateDto,
    DadosHidraulicosSubsistemaCreateDto,
)

logger = logging.getLogger(__name__)


__DB__ = db_mysql_master('db_ons_dados_abertos')


class DadosHidraulicosUhe:
    tb: db.Table = __DB__.getSchema('dados_hidraulicos_uhe')
    
    
    @staticmethod
    def delete_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date
    ):
        if not data_fim:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.delete(DadosHidraulicosUhe.tb).where(
            query_param
        )
        

        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} dados hidraulicos UHE removidos entre {data_inicio} e {data_fim}")
        return None
    
    @staticmethod
    def create(body: List[DadosHidraulicosUheCreateDto]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)
        data_max = df['data_referente'].max()
        data_min = df['data_referente'].min()
        df_dados_atuais = pd.DataFrame(DadosHidraulicosUhe.get_by_data_referente_entre(data_min, data_max))
        if not df_dados_atuais.empty:
            df = df.combine_first(df_dados_atuais)
        DadosHidraulicosUhe.delete_by_data_referente_entre(data_min, data_max)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        query = db.insert(DadosHidraulicosUhe.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query).rowcount
        logger.info(f"{result} dados hidraulicos UHE inseridos entre {data_min} e {data_max}")
        return body_dict

    
    @staticmethod
    def get_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date | None = None
    ):
        
        if not data_fim:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.select(DadosHidraulicosUhe.tb).where(
            query_param
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            return [{}]
            raise HTTPException(
                status_code=404, detail=f"Dados hidraulicos UHE entre {data_inicio} e {data_fim} nao encontrados"
            )

        df = pd.DataFrame(result, columns=DadosHidraulicosUhe.tb.columns.keys())
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
class DadosHidraulicosSubsistema:
    tb: db.Table = __DB__.getSchema('dados_hidraulicos_subsistema')
    
    @staticmethod
    def delete_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date
    ):
        query = db.delete(DadosHidraulicosSubsistema.tb).where(
            DadosHidraulicosSubsistema.tb.c.data_referente.between(data_inicio, data_fim)
        )
        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} dados hidraulicos subsistema removidos entre {data_inicio} e {data_fim}")
        return None
    
    @staticmethod
    def create(body: List[DadosHidraulicosSubsistemaCreateDto]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)
        data_max = df['data_referente'].max()
        data_min = df['data_referente'].min()
        df_dados_atuais = pd.DataFrame(DadosHidraulicosSubsistema.get_by_data_referente_entre(data_min, data_max))
        if not df_dados_atuais.empty:
            df = df.combine_first(df_dados_atuais)
        DadosHidraulicosSubsistema.delete_by_data_referente_entre(data_min, data_max)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        query = db.insert(DadosHidraulicosSubsistema.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query).rowcount
        logger.info(f"{result} dados hidraulicos subsistema inseridos entre {data_min} e {data_max}")
        return body_dict

    
    @staticmethod
    def get_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date | None = None
    ):
        if not data_fim:
            query_param = (DadosHidraulicosSubsistema.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosSubsistema.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.select(DadosHidraulicosSubsistema.tb).where(
            query_param
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            return [{}]
        df = pd.DataFrame(result, columns=DadosHidraulicosSubsistema.tb.columns.keys())
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
