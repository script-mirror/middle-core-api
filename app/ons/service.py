from sys import path
import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
from typing import List
from app.core.utils import date_util
from fastapi import HTTPException
from app.core.database.wx_dbClass import db_mysql_master
from app.core.utils.logger import logging
from typing import List, Optional

from .schema import (
    CargaIpdoCreateDto,
    PrevisaoDiariaEnaCreateDto,
    PrevisaoSemanalEnaCreateDto,
    PrevisaoSemanalEnaPorBaciaCreateDto
)
__DB__ = db_mysql_master('db_ons')
logger = logging.getLogger(__name__)


class BaciasSegmentadas:
    tb: db.Table = __DB__.getSchema('tb_bacias_segmentadas')

    @staticmethod
    def get_bacias_segmentadas():
        query = db.select(
            BaciasSegmentadas.tb.c['cd_bacia'],
            BaciasSegmentadas.tb.c['str_bacia'],
            BaciasSegmentadas.tb.c['cd_submercado']
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result, columns=['cd_bacia', 'str_bacia', 'cd_submercado'])
        return df.to_dict('records')

    @staticmethod
    def get_bacias_segmentadas_by_cd_bacia(cd_bacia):
        query = db.select(
            BaciasSegmentadas.tb.c['cd_bacia'],
            BaciasSegmentadas.tb.c['cd_submercado'],
        ).where(
            BaciasSegmentadas.tb.c['cd_bacia'] == cd_bacia
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['cd_bacia', 'cd_submercado'])
        return df.to_dict('records')


class tb_bacias:
    tb: db.Table = __DB__.getSchema('tb_bacias')

    @staticmethod
    def get_bacias(divisao: str):
        query = db.select(
            tb_bacias.tb.c['id_bacia'],
            tb_bacias.tb.c['str_bacia'],
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['id', 'nome'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        if divisao == 'tb_chuva':
            df = df[~df['nome'].isin(
                ['AMAZONAS', 'PARAGUAI', 'PARAÍBA_DO_SUL', 'SÃO_FRANCISCO', 'TOCANTINS'])]
        return df.to_dict('records')


class tb_submercado:
    tb: db.Table = __DB__.getSchema('tb_submercado')

    @staticmethod
    def get_submercados():
        query = db.select(
            tb_submercado.tb.c['cd_submercado'],
            tb_submercado.tb.c['str_submercado'],
            tb_submercado.tb.c['str_sigla']
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['id', 'nome', 'str_sigla'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')


class Acomph:
    tb: db.Table = __DB__.getSchema('tb_acomph')

    @staticmethod
    def get_acomph_by_dt_referente(data: datetime.date):
        inner_query = db.select(
            Acomph.tb.c['cd_posto'],
            Acomph.tb.c['dt_referente'],
            Acomph.tb.c['vl_vaz_inc_conso'],
            Acomph.tb.c['vl_vaz_nat_conso'],
            Acomph.tb.c['dt_acomph'],
            db.func.row_number().over(
                partition_by=[Acomph.tb.c['cd_posto'],
                              Acomph.tb.c['dt_referente']],
                order_by=Acomph.tb.c['dt_acomph'].desc()
            ).label('row_number')
        ).where(
            Acomph.tb.c['dt_referente'] >= data
        ).alias('_')

        query = db.select(
            inner_query.c['cd_posto'],
            db.func.date(inner_query.c['dt_referente']),
            inner_query.c['vl_vaz_inc_conso'],
            inner_query.c['vl_vaz_nat_conso'],
            db.func.date(inner_query.c['dt_acomph'])
        ).where(
            inner_query.c['row_number'] == 1
        )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['cd_posto', 'dt_referente', 'vl_vaz_inc_conso',
                                           'vl_vaz_nat_conso', 'dt_acomph'])
        return df.to_dict('records')

    @staticmethod
    def get_acomph_by_data_produto(data: datetime.date):
        query = db.select(
            db.func.date(Acomph.tb.c['dt_referente']),
            Acomph.tb.c['cd_posto'],
            Acomph.tb.c['vl_vaz_def_conso'],
            Acomph.tb.c['vl_vaz_inc_conso'],
            Acomph.tb.c['vl_vaz_nat_conso'],
            db.func.date(Acomph.tb.c['dt_acomph'])
        ).where(
            db.func.date(Acomph.tb.c['dt_acomph']) == data
        )
        result = __DB__.db_execute(query).fetchall()

        if not result:
            raise HTTPException(
                status_code=404, detail=f"Acomph da data {data} não encontrado")

        df = pd.DataFrame(result, columns=['dt_referente', 'cd_posto', 'vl_vaz_def_conso',
                                           'vl_vaz_inc_conso', 'vl_vaz_nat_conso', 'dt_acomph'])
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')

    @staticmethod
    def get_available_dt_acomph_by_year(year: int):
        query = db.select(
            db.func.distinct(db.func.date(Acomph.tb.c['dt_acomph']))
        ).where(
            Acomph.tb.c['dt_acomph'].between(f'{year}-01-01', f'{year}-12-31')
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            raise HTTPException(
                status_code=404, detail=f"Não foram encontradas datas de Acomph para o ano {year}")

        df = pd.DataFrame(result, columns=['dt_acomph'])
        return [str(x) for x in df['dt_acomph'].tolist()]

    @staticmethod
    def post_acomph(body: List[dict]):
        df = pd.DataFrame(body)
        df.drop_duplicates(subset=['dt_referente', 'cd_posto'], inplace=True)
        df.sort_values(['dt_referente', 'cd_posto'], inplace=True)
        df = df.replace({np.nan: -1, np.inf: -1, -np.inf: -1})
        Acomph.delete_acomph_by_dt_acomph(df['dt_acomph'].unique().tolist()[0])
        query = db.insert(Acomph.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        AcomphConsolidado.post_acomph_consolidado(df.to_dict('records'))
        return {"inserts": result.rowcount}

    @staticmethod
    def delete_acomph_by_dt_acomph(dt_acomph: datetime.date):
        query = db.delete(Acomph.tb).where(
            Acomph.tb.c['dt_acomph'] == dt_acomph
        )
        result = __DB__.db_execute(query)
        return {"deletes": result.rowcount}


class AcomphConsolidado:
    tb: db.Table = __DB__.getSchema('acomph_consolidado')

    @staticmethod
    def post_acomph_consolidado(body: List[dict]):
        df = pd.DataFrame(body)
        AcomphConsolidado.delete_acomph_consolidado_by_dt_referente_between(
            df['dt_referente'].min(), df['dt_referente'].max())
        query = db.insert(AcomphConsolidado.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}

    @staticmethod
    def delete_acomph_consolidado_by_dt_referente_between(
        dt_referente_inicial: datetime.date,
        dt_referente_final: datetime.date
    ):
        query = db.delete(AcomphConsolidado.tb).where(
            AcomphConsolidado.tb.c['dt_referente'].between(
                dt_referente_inicial, dt_referente_final)
        )
        result = __DB__.db_execute(query)
        return {"deletes": result.rowcount}

    @staticmethod
    def get_vazao_by_data_entre(
        inicio: datetime.date,
        fim: datetime.date = None
    ):
        query = db.select(
            AcomphConsolidado.tb.c['dt_referente'],
            AcomphConsolidado.tb.c['cd_posto'],
            AcomphConsolidado.tb.c['vl_vaz_def_conso'],
            AcomphConsolidado.tb.c['vl_vaz_inc_conso'],
            AcomphConsolidado.tb.c['vl_vaz_nat_conso']
        )

        if fim is None:
            query = query.where(
                AcomphConsolidado.tb.c['dt_referente'] >= inicio)
        else:
            query = query.where(
                AcomphConsolidado.tb.c['dt_referente'].between(inicio, fim)
            )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'dt_referente', 'cd_posto', 'vl_vaz_def_conso',
                'vl_vaz_inc_conso', 'vl_vaz_nat_conso'
            ]
        )
        return df.to_dict('records')


class EnaAcomph:
    tb: db.Table = __DB__.getSchema('ena_acomph')

    @staticmethod
    def delete_ena_acomph_by_dates(datas: List[datetime.date]):
        query = db.delete(EnaAcomph.tb).where(
            EnaAcomph.tb.c['data'].in_(datas)
        )
        result = __DB__.db_execute(query)
        return {"deletes": result.rowcount}

    @staticmethod
    def post_ena_acomph(ena_acomph: List[dict]):
        df = pd.DataFrame(ena_acomph)
        df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
        df['data'] = df['data'].dt.date
        EnaAcomph.delete_ena_acomph_by_dates(df['data'].unique().tolist())
        query = db.insert(EnaAcomph.tb).values(ena_acomph)
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}

    @staticmethod
    def get_ena_acomph_by_granularidade_date_between(
        granularidade: str, data_inicial: datetime.date, data_final: datetime.date
    ):
        query = db.select(
            EnaAcomph.tb.c['data'],
            EnaAcomph.tb.c['localizacao'],
            EnaAcomph.tb.c['ena']
        ).where(
            EnaAcomph.tb.c['granularidade'] == granularidade,
            EnaAcomph.tb.c['data'].between(data_inicial, data_final)
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['data', granularidade, 'ena'])
        return df.to_dict('records')


class VeBacias:
    tb: db.Table = __DB__.getSchema('tb_ve_bacias')

    @staticmethod
    def get_ve_bacias(dt_inicio_semana: datetime.date):
        query = db.select(
            VeBacias.tb.c['cd_bacia'],
            VeBacias.tb.c['vl_mes'],
            VeBacias.tb.c['dt_inicio_semana'],
            VeBacias.tb.c['cd_revisao'],
            (
                (VeBacias.tb.c['vl_ena'] * 100)
                / db.func.nullif(VeBacias.tb.c['vl_perc_mlt'], 0)
            ).label('mlt')
        ).where(VeBacias.tb.c['dt_inicio_semana'] >= dt_inicio_semana)

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'cd_bacia', 'vl_mes', 'dt_inicio_semana', 'cd_revisao', 'mlt'
        ])
        return df.to_dict('records')


class TipoGeracao:
    tb: db.Table = __DB__.getSchema('tb_tipo_geracao')


class GeracaoHoraria:
    tb: db.Table = __DB__.getSchema('tb_geracao_horaria')

    @staticmethod
    def get_geracao_horaria(dt_update: datetime.date):
        query = db.select(
            GeracaoHoraria.tb.c['str_submercado'],
            GeracaoHoraria.tb.c['dt_referente'],
            GeracaoHoraria.tb.c['vl_carga'],
            TipoGeracao.tb.c['str_geracao'],
        ).join(
            TipoGeracao.tb, GeracaoHoraria.tb.c['cd_geracao'] == TipoGeracao.tb.c['cd_geracao']
        ).where(
            GeracaoHoraria.tb.c['dt_update'] == f"{dt_update} 00:00:00"
        ).order_by(
            TipoGeracao.tb.c['str_geracao'],
            GeracaoHoraria.tb.c['dt_referente']
        )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'submercado', 'dt_referente', 'vl_carga', 'tipo_geracao'
        ])
        df['dt_referente'] = pd.to_datetime(df['dt_referente'])
        df['hora'] = df['dt_referente'].dt.floor('h')
        df_agrupado = df.groupby(['submercado', 'tipo_geracao', 'hora'])[
            'vl_carga'].mean().reset_index()
        df_agrupado = df_agrupado.rename(columns={'hora': 'dt_referente'})

        return df_agrupado.to_dict('records')

    @staticmethod
    def get_geracao_horaria_data_entre(inicio: datetime.date, fim: datetime.date):
        query = db.select(
            GeracaoHoraria.tb.c['str_submercado'],
            GeracaoHoraria.tb.c['dt_referente'],
            GeracaoHoraria.tb.c['vl_carga'],
            TipoGeracao.tb.c['str_geracao'],
        ).join(
            TipoGeracao.tb, GeracaoHoraria.tb.c['cd_geracao'] == TipoGeracao.tb.c['cd_geracao']
        ).where(
            GeracaoHoraria.tb.c['dt_update'].between(
                f"{inicio} 00:00:00", f"{fim} 00:00:00")
        ).order_by(
            TipoGeracao.tb.c['str_geracao'],
            GeracaoHoraria.tb.c['dt_referente']
        )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'submercado', 'dt_referente', 'vl_carga', 'tipo_geracao'
        ])
        df['dt_referente'] = pd.to_datetime(df['dt_referente'])
        df['hora'] = df['dt_referente'].dt.floor('h')
        df_agrupado = df.groupby(['submercado', 'tipo_geracao', 'hora'])[
            'vl_carga'].mean().reset_index()
        df_agrupado = df_agrupado.rename(columns={'hora': 'dt_referente'})

        return df_agrupado.to_dict('records')


class CargaHoraria:
    tb: db.Table = __DB__.getSchema('tb_carga_horaria')

    @staticmethod
    def get_carga_horaria(dt_update: datetime.date):
        query = db.select(
            CargaHoraria.tb.c['str_submercado'],
            CargaHoraria.tb.c['dt_referente'],
            CargaHoraria.tb.c['vl_carga'],
        ).where(
            CargaHoraria.tb.c['dt_update'] == f"{dt_update} 00:00:00"
        ).order_by(
            CargaHoraria.tb.c['dt_referente']
        )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'submercado', 'dt_referente', 'vl_carga'
        ])
        df['dt_referente'] = pd.to_datetime(df['dt_referente'])
        df['hora'] = df['dt_referente'].dt.floor('h')
        df_agrupado = df.groupby(['submercado', 'hora'])[
            'vl_carga'].mean().reset_index()
        df_agrupado = df_agrupado.rename(columns={'hora': 'dt_referente'})

        return df_agrupado.to_dict('records')

    @staticmethod
    def get_carga_horaria_data_entre(inicio: datetime.date, fim: datetime.date):
        query = db.select(
            CargaHoraria.tb.c['str_submercado'],
            CargaHoraria.tb.c['dt_referente'],
            CargaHoraria.tb.c['vl_carga'],
        ).where(
            CargaHoraria.tb.c['dt_update'].between(
                f"{inicio} 00:00:00", f"{fim} 00:00:00")
        ).order_by(
            CargaHoraria.tb.c['dt_referente']
        )

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'submercado', 'dt_referente', 'vl_carga'
        ])
        df['dt_referente'] = pd.to_datetime(df['dt_referente'])
        df['hora'] = df['dt_referente'].dt.floor('h')
        df_agrupado = df.groupby(['submercado', 'hora'])[
            'vl_carga'].mean().reset_index()
        df_agrupado = df_agrupado.rename(columns={'hora': 'dt_referente'})

        return df_agrupado.to_dict('records')


class Produtibilidade:
    tb: db.Table = __DB__.getSchema('tb_produtibilidade')

    @staticmethod
    def get_all():
        query = db.select(
            Produtibilidade.tb.c['cd_posto'],
            Produtibilidade.tb.c['str_posto'],
            Produtibilidade.tb.c['vl_produtibilidade'],
            Produtibilidade.tb.c['cd_submercado'],
            Produtibilidade.tb.c['str_submercado'],
            Produtibilidade.tb.c['str_sigla'],
            Produtibilidade.tb.c['cd_bacia'],
            Produtibilidade.tb.c['str_bacia'],
            Produtibilidade.tb.c['cd_ree'],
            Produtibilidade.tb.c['str_ree']
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'cd_posto', 'str_posto', 'vl_produtibilidade', 'cd_submercado',
            'str_submercado', 'str_sigla', 'cd_bacia', 'str_bacia',
            'cd_ree', 'str_ree'
        ])
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')


class CargaIpdo:
    tb: db.Table = __DB__.getSchema('tb_carga_ipdo')

    @staticmethod
    def get_carga_ipdo(data_referente: datetime.date):
        query = db.select(
            CargaIpdo.tb.c['dt_referente'],
            CargaIpdo.tb.c['carga_se'],
            CargaIpdo.tb.c['carga_s'],
            CargaIpdo.tb.c['carga_ne'],
            CargaIpdo.tb.c['carga_n'],
        ).where(
            CargaIpdo.tb.c['dt_referente'] == data_referente
        )
        result = __DB__.db_execute(query).fetchall()
        if not result:
            raise HTTPException(
                status_code=404, detail=f"Carga IPDO da data {data_referente} nao encontrada")
        df = pd.DataFrame(result, columns=[
            'dt_referente', 'carga_se', 'carga_s', 'carga_ne', 'carga_n'
        ])
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
    @staticmethod
    def delete_carga_ipdo_by_dt_referente(data_referente: datetime.date | str):
        query = db.delete(CargaIpdo.tb).where(
            CargaIpdo.tb.c['dt_referente'] == data_referente
        )
        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} cargas IPDO removidas da data {data_referente}")
        return {"deletes": result.rowcount}
    
    @staticmethod
    def post_carga_ipdo(body: CargaIpdoCreateDto):
        body = body.model_dump()
        CargaIpdo.delete_carga_ipdo_by_dt_referente(body['dt_referente'])
        query = db.insert(CargaIpdo.tb).values([body])
        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} cargas IPDO inseridas da data {body['dt_referente']}")
        return {"inserts": result.rowcount}


class Rdh:
    tb: db.Table = __DB__.getSchema('tb_rdh')

    @staticmethod
    def get_rdh_by_dt_referente(dt_referente: datetime.date):
        query = db.select(
            Rdh.tb.c["cd_posto"],
            Rdh.tb.c["vl_vol_arm_perc"],
            Rdh.tb.c["vl_mlt_vaz"],
            Rdh.tb.c["vl_vaz_dia"],
            Rdh.tb.c["vl_vaz_turb"],
            Rdh.tb.c["vl_vaz_vert"],
            Rdh.tb.c["vl_vaz_dfl"],
            Rdh.tb.c["vl_vaz_transf"],
            Rdh.tb.c["vl_vaz_afl"],
            Rdh.tb.c["vl_vaz_inc"],
            Rdh.tb.c["vl_vaz_consunt"],
            Rdh.tb.c["vl_vaz_evp"],
            Rdh.tb.c["dt_referente"]
        ).where(
            Rdh.tb.c['dt_referente'] == dt_referente
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            "cd_posto",
            "vl_vol_arm_perc",
            "vl_mlt_vaz",
            "vl_vaz_dia",
            "vl_vaz_turb",
            "vl_vaz_vert",
            "vl_vaz_dfl",
            "vl_vaz_transf",
            "vl_vaz_afl",
            "vl_vaz_inc",
            "vl_vaz_consunt",
            "vl_vaz_evp",
            "dt_referente",
        ])
        return df.replace(
            {np.nan: None, np.inf: None, -np.inf: None}
            ).to_dict('records')

    @staticmethod
    def remove_rdh_by_dt_referente(dt_referente: datetime.date):
        data_str = dt_referente.strftime('%Y-%m-%d 00:00:00')
        query = db.delete(Rdh.tb).where(
            Rdh.tb.c['dt_referente'] == data_str
        )
        result = __DB__.db_execute(query)
        return {"deletes": result.rowcount}

    @staticmethod
    def post_rdh(
        body: List[dict]
    ):
        """
        Insere ou atualiza os dados de RDH.
        """
        df = pd.DataFrame(body)
        Rdh.remove_rdh_by_dt_referente(df['dt_referente'].unique().tolist()[0])
        query = db.insert(Rdh.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}


class EnaSubmercado:
    tb: db.Table = __DB__.getSchema('tb_ena_submercado')
    
    def get_mlt():
        
        cod_submercados = {1:'SE',2:'S',3:'NE',4:'N'}
        
        query = db.select(
                EnaSubmercado.tb.c['id_submercado'],
                EnaSubmercado.tb.c['dt_ref'],
                EnaSubmercado.tb.c['vl_mwmed'],
                EnaSubmercado.tb.c['vl_percent']
            ).where(
                EnaSubmercado.tb.c['dt_ref'].between('2022-01-01','2022-12-31')
            )
        df = pd.DataFrame(
            __DB__.db_execute(query).fetchall(),
            columns=['cd_submercado','data_referente','mwmed','porcentagem_mlt']
        )
        df['cd_submercado'] = df['cd_submercado'].replace(cod_submercados)
        df['data_referente'] = pd.to_datetime(df['data_referente'])
        df['mes'] = df['data_referente'].dt.month
        df['mlt'] = ((df['mwmed'] * 100) / df['porcentagem_mlt']).astype(int)
        df.drop(columns=['data_referente'], inplace=True)
        df.pivot(index='cd_submercado', columns='mes', values='mlt').reset_index().reset_index(drop=1)
        
        return df[['cd_submercado', 'mes', 'mlt']].to_dict('records')
    

class EnaBacia:
    tb: db.Table = __DB__.getSchema('tb_ena_bacia')
    def get_mlt():
        query = db.select(
                EnaBacia.tb.c['id_bacia'],
                EnaBacia.tb.c['dt_ref'],
                EnaBacia.tb.c['vl_mwmed'],
                EnaBacia.tb.c['vl_percent']
            ).where(
                EnaBacia.tb.c['dt_ref'].between('2022-01-01','2022-12-31')
            )
        df = pd.DataFrame(
            __DB__.db_execute(query).fetchall(),
            columns=['id_bacia','data_referente','mwmed','porcentagem_mlt']
        )
        df['id_bacia'] = df['id_bacia'].replace('')
        df['data_referente'] = pd.to_datetime(df['data_referente'])
        df['mes'] = df['data_referente'].dt.month
        df['mlt'] = ((df['mwmed'] * 100) / df['porcentagem_mlt']).astype(int)
        df.drop(columns=['data_referente'], inplace=True)
        df.pivot(index='id_bacia', columns='mes', values='mlt').reset_index().reset_index(drop=1)
        
        return df[['id_bacia', 'mes', 'mlt']].to_dict('records')


class PrevisoesDiarias:
    tb: db.Table = __DB__.getSchema('tb_prev_ena_submercado')
    
    def _delete_prev_diaria_ena_by_equal_dates(dt_previsao: datetime.date):
        data_str = dt_previsao.strftime('%Y-%m-%d 00:00:00')
        query = db.delete(PrevisoesDiarias.tb).where(
            PrevisoesDiarias.tb.c['dt_previsao'] == data_str
        )
        result = __DB__.db_execute(query)
        return {"deletes:": result.rowcount}
    
    def post_prev_diaria_ena(
        body: List[PrevisaoDiariaEnaCreateDto]
    ):
        df = pd.DataFrame([item.model_dump() for item in body])
        PrevisoesDiarias._delete_prev_ena_by_equal_dates(df['dt_previsao'].unique().tolist()[0])
        query = db.insert(PrevisoesDiarias.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}
        
    def get_prev_diaria_ena(dt_revisao: Optional[datetime.date] = None, submercado: Optional[int] = None): 
        if dt_revisao is None:
            
            subquery = db.select(
                db.func.max(PrevisoesDiarias.tb.c['dt_previsao'])
            ).scalar_subquery()
            
            query = db.select(
                PrevisoesDiarias.tb.c['cd_submercado'],
                PrevisoesDiarias.tb.c['dt_previsao'],
                PrevisoesDiarias.tb.c['dt_ref'],
                PrevisoesDiarias.tb.c['vl_mwmed'],
                PrevisoesDiarias.tb.c['vl_perc_mlt']
            ).where(
                PrevisoesDiarias.tb.c['dt_previsao'] == subquery
            )
            
        else:
            
            query = db.select(
            PrevisoesDiarias.tb.c['cd_submercado'],
            PrevisoesDiarias.tb.c['dt_previsao'],
            PrevisoesDiarias.tb.c['dt_ref'],
            PrevisoesDiarias.tb.c['vl_mwmed'],
            PrevisoesDiarias.tb.c['vl_perc_mlt']
            )
            
            query = query.where(
                PrevisoesDiarias.tb.c['dt_previsao'] == dt_revisao
            )
            
        if submercado:
            query = query.where(
                PrevisoesDiarias.tb.c['cd_submercado'] == submercado
            )
            
        result = __DB__.db_execute(query).fetchall()
        
        df = pd.DataFrame(result, columns=[
            'cd_submercado', 'dt_previsao', 'dt_ref', 'vl_mwmed', 'vl_perc_mlt'
        ])
        
        if df.empty:
            raise HTTPException(
                status_code=404, detail="Nenhum registro encontrado com os filtros informados"
            )
            
        df['dt_previsao'] = pd.to_datetime(df['dt_previsao']).dt.date
        
        df['dt_ref'] = pd.to_datetime(df['dt_ref']).dt.date
        
        return df.to_dict('records')     


class PrevisoesSemanais:
    tb_ve: db.Table = __DB__.getSchema('tb_ve')
    tb_ve_bacias: db.Table = __DB__.getSchema('tb_ve_bacias')
    
    def _delete_prev_semanal_by_equal_dates(ano: int, mes: int):
        
        query = db.delete(PrevisoesSemanais.tb_ve).where(
            PrevisoesSemanais.tb_ve.c['vl_ano'] == ano,
            PrevisoesSemanais.tb_ve.c['vl_mes'] == mes
        )
        result = __DB__.db_execute(query)
        return {"deletes:": result.rowcount}
    
    def _delete_prev_semanal_por_bacia_by_equal_dates(ano: int, mes: int):
        
        query = db.delete(PrevisoesSemanais.tb_ve_bacias).where(
            PrevisoesSemanais.tb_ve_bacias.c['vl_ano'] == ano,
            PrevisoesSemanais.tb_ve_bacias.c['vl_mes'] == mes
        )
        result = __DB__.db_execute(query)
        return {"deletes:": result.rowcount}
    
    def post_prev_semanal_ena(
        body: List[PrevisaoSemanalEnaCreateDto]
    ):
        df = pd.DataFrame([item.model_dump() for item in body])
        PrevisoesSemanais._delete_prev_semanal_by_equal_dates(df['vl_ano'].unique().tolist()[0],df['vl_mes'].unique().tolist()[0])
        query = db.insert(PrevisoesSemanais.tb_ve).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}
    
    def get_prev_semanal_ena(
        dt_inicio_semana: Optional[datetime.date] = None,
        ano: Optional[int] = None,
        mes: Optional[int] = None,
        cd_submercado: Optional[int] = None
    ): 
        # Validação: se passar mês, deve passar ano
        if mes is not None and ano is None:
            raise HTTPException(
                status_code=400,
                detail="Para filtrar por mês, o ano também deve ser informado"
            )

        # Se nenhum filtro foi passado, busca a semana mais recente
        if all(param is None for param in [dt_inicio_semana, ano, mes]):
            subquery = db.select(
                db.func.max(PrevisoesSemanais.tb_ve.c['dt_inicio_semana'])
            ).scalar_subquery()

            query = db.select(
                PrevisoesSemanais.tb_ve.c['vl_ano'],
                PrevisoesSemanais.tb_ve.c['vl_mes'],
                PrevisoesSemanais.tb_ve.c['cd_revisao'],
                PrevisoesSemanais.tb_ve.c['cd_submercado'],
                PrevisoesSemanais.tb_ve.c['dt_inicio_semana'],
                PrevisoesSemanais.tb_ve.c['vl_ena']
            ).where(
                PrevisoesSemanais.tb_ve.c['dt_inicio_semana'] == subquery
            )
        else:
            query = db.select(
                PrevisoesSemanais.tb_ve.c['vl_ano'],
                PrevisoesSemanais.tb_ve.c['vl_mes'],
                PrevisoesSemanais.tb_ve.c['cd_revisao'],
                PrevisoesSemanais.tb_ve.c['cd_submercado'],
                PrevisoesSemanais.tb_ve.c['dt_inicio_semana'],
                PrevisoesSemanais.tb_ve.c['vl_ena']
            )

            # Aplica filtros condicionalmente
            if dt_inicio_semana is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve.c['dt_inicio_semana'] == dt_inicio_semana
                )

            if ano is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve.c['vl_ano'] == ano
                )

            if mes is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve.c['vl_mes'] == mes
                )

        # Filtro de submercado (independente)
        if cd_submercado is not None:
            query = query.where(
                PrevisoesSemanais.tb_ve.c['cd_submercado'] == cd_submercado
            )

        # Ordena por data para facilitar visualização
        query = query.order_by(
            PrevisoesSemanais.tb_ve.c['dt_inicio_semana'].desc(),
            PrevisoesSemanais.tb_ve.c['cd_submercado']
        )

        result = __DB__.db_execute(query).fetchall()

        df = pd.DataFrame(result, columns=[
            'vl_ano', 'vl_mes', 'cd_revisao', 'cd_submercado', 'dt_inicio_semana', 'vl_ena'
        ])

        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="Nenhum registro encontrado com os filtros informados"
            )

        df['dt_inicio_semana'] = pd.to_datetime(df['dt_inicio_semana']).dt.date

        return df.to_dict('records')
    
    def post_prev_semanal_ena_por_bacia(
        body: List[PrevisaoSemanalEnaPorBaciaCreateDto]
    ):
        df = pd.DataFrame([item.model_dump() for item in body])
        PrevisoesSemanais._delete_prev_semanal_por_bacia_by_equal_dates(df['vl_ano'].unique().tolist()[0],df['vl_mes'].unique().tolist()[0])
        query = db.insert(PrevisoesSemanais.tb_ve_bacias).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts": result.rowcount}
    
    def get_prev_semanal_ena_por_bacia(
        dt_inicio_semanal: Optional[datetime.date] = None,
        ano: Optional[int] = None,
        mes: Optional[int] = None,
        cd_bacia: Optional[int] = None
    ):
        # Validação: se passar mês, deve passar ano
        if mes is not None and ano is None:
            raise HTTPException(
                status_code=400,
                detail="Para filtrar por mês, o ano também deve ser informado"
            )

        # Se nenhum filtro foi passado, busca a semana mais recente
        if all(param is None for param in [dt_inicio_semanal, ano, mes]):
            subquery = db.select(
                db.func.max(PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'])
            ).scalar_subquery()

            query = db.select(
                PrevisoesSemanais.tb_ve_bacias.c['vl_ano'],
                PrevisoesSemanais.tb_ve_bacias.c['vl_mes'],
                PrevisoesSemanais.tb_ve_bacias.c['cd_revisao'],
                PrevisoesSemanais.tb_ve_bacias.c['cd_bacia'],
                PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'],
                PrevisoesSemanais.tb_ve_bacias.c['vl_ena']
            ).where(
                PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'] == subquery
            )
        else:
            query = db.select(
                PrevisoesSemanais.tb_ve_bacias.c['vl_ano'],
                PrevisoesSemanais.tb_ve_bacias.c['vl_mes'],
                PrevisoesSemanais.tb_ve_bacias.c['cd_revisao'],
                PrevisoesSemanais.tb_ve_bacias.c['cd_bacia'],
                PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'],
                PrevisoesSemanais.tb_ve_bacias.c['vl_ena']
            )

            # Aplica filtros condicionalmente
            if dt_inicio_semanal is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'] == dt_inicio_semanal
                )

            if ano is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve_bacias.c['vl_ano'] == ano
                )

            if mes is not None:
                query = query.where(
                    PrevisoesSemanais.tb_ve_bacias.c['vl_mes'] == mes
                )

        # Filtro de bacia (independente)
        if cd_bacia is not None:
            query = query.where(
                PrevisoesSemanais.tb_ve_bacias.c['cd_bacia'] == cd_bacia
            )
            
        # Ordena por data para facilitar visualização
        query = query.order_by(
            PrevisoesSemanais.tb_ve_bacias.c['dt_inicio_semana'].desc(),
            PrevisoesSemanais.tb_ve_bacias.c['cd_bacia']
        )
        
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=[
            'vl_ano', 'vl_mes', 'cd_revisao', 'cd_bacia', 'dt_inicio_semana', 'vl_ena'
        ])
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="Nenhum registro encontrado com os filtros informados"
            )
        df['dt_inicio_semana'] = pd.to_datetime(df['dt_inicio_semana']).dt.date
        
        return df.to_dict('records')
   
if __name__ == "__main__":
    teste = [
        'tb_bacias',
        'tb_bacias',
        'tb_chuva',
        'tb_submercado',
        'tb_submercado',
    ]
    print(set(teste))
    pass


    