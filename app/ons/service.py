from sys import path
import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
from typing import List
from app.core.utils import date_util

from app.core.database.wx_dbClass import db_mysql_master

__DB__ = db_mysql_master('db_ons')



    
class BaciasSegmentadas:
    tb:db.Table = __DB__.getSchema('tb_bacias_segmentadas')

    @staticmethod
    def get_bacias_segmentadas():
        query = db.select(
          BaciasSegmentadas.tb.c['cd_bacia'],
          BaciasSegmentadas.tb.c['str_bacia'],
          BaciasSegmentadas.tb.c['cd_submercado']
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['cd_bacia','str_bacia', 'cd_submercado'])
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
        df = pd.DataFrame(result, columns=['cd_bacia','cd_submercado'])
        return df.to_dict('records')

class tb_bacias:
    tb:db.Table = __DB__.getSchema('tb_bacias')
    
    @staticmethod
    def get_bacias(divisao:str):
        query = db.select(
          tb_bacias.tb.c['id_bacia'],
          tb_bacias.tb.c['str_bacia'],
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['id','nome'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        if divisao == 'tb_chuva':
            df = df[~df['nome'].isin(['AMAZONAS','PARAGUAI','PARAÍBA_DO_SUL','SÃO_FRANCISCO','TOCANTINS'])]
        return df.to_dict('records')

class tb_submercado:
    tb:db.Table = __DB__.getSchema('tb_submercado')
    
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
        
class VeBacias:
    tb:db.Table = __DB__.getSchema('tb_ve_bacias')
    

class Acomph:
    tb:db.Table = __DB__.getSchema('tb_acomph')
    @staticmethod
    def get_acomph_by_dt_referente(data:datetime.date):
        inner_query = db.select(
            Acomph.tb.c['cd_posto'],
            Acomph.tb.c['dt_referente'],
            Acomph.tb.c['vl_vaz_inc_conso'],
            Acomph.tb.c['vl_vaz_nat_conso'],
            Acomph.tb.c['dt_acomph'],
            db.func.row_number().over(
                partition_by=[Acomph.tb.c['cd_posto'], Acomph.tb.c['dt_referente']],
                order_by=Acomph.tb.c['dt_acomph'].desc()
            ).label('row_number')
        ).where(
            Acomph.tb.c['dt_referente'] >= data
        ).alias('_')
        
        query = db.select(
            inner_query.c.cd_posto,
            db.func.date(inner_query.c.dt_referente),
            inner_query.c.vl_vaz_inc_conso,
            inner_query.c.vl_vaz_nat_conso,
            db.func.date(inner_query.c.dt_acomph)
        ).where(
            inner_query.c.row_number == 1
        )
        
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['cd_posto', 'dt_referente', 'vl_vaz_inc_conso', 
                                          'vl_vaz_nat_conso', 'dt_acomph'])
        return df.to_dict('records')
    
    @staticmethod
    def get_acomph_by_dt_acomph(data:datetime.date):
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
        df = pd.DataFrame(result, columns=['dt_referente', 'cd_posto', 'vl_vaz_def_conso', 
                                          'vl_vaz_inc_conso', 'vl_vaz_nat_conso', 'dt_acomph'])
        return df.to_dict('records')

class AcomphHistorico:
    tb:db.Table = __DB__.getSchema('acomph_historico')

    @staticmethod
    def post_acomph_historico(body:List[dict]):
        df = pd.DataFrame(body)
        df.drop_duplicates(subset=['dt_referente', 'cd_posto'], inplace=True)
        df.sort_values(['dt_referente', 'cd_posto'], inplace=True)
        query = db.insert(AcomphHistorico.tb).values(body)
        result = __DB__.db_execute(query)
        AcomphConsolidado.post_acomph_consolidado(body)
        return {"inserts":result.rowcount}
    
class AcomphConsolidado:
    tb:db.Table = __DB__.getSchema('acomph_consolidado')
    
    @staticmethod
    def post_acomph_consolidado(body:List[dict]):
        df = pd.DataFrame(body)
        df.drop(columns=['dt_acomph'], inplace=True)
        AcomphConsolidado.delete_acomph_consolidado_by_dt_referente_between(df['dt_referente'].min(), df['dt_referente'].max())
        query = db.insert(AcomphConsolidado.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query)
        return {"inserts":result.rowcount}
    
    @staticmethod
    def delete_acomph_consolidado_by_dt_referente_between(dt_referente_inicial:datetime.date, dt_referente_final:datetime.date):
        query = db.delete(AcomphConsolidado.tb).where(
            AcomphConsolidado.tb.c['dt_referente'].between(dt_referente_inicial, dt_referente_final)
        )
        result = __DB__.db_execute(query)
        return {"deletes":result.rowcount}
class EnaAcomph:
    tb:db.Table = __DB__.getSchema('ena_acomph')
    
    @staticmethod
    def delete_ena_acomph_by_dates(datas:List[datetime.date]):
        query = db.delete(EnaAcomph.tb).where(
            EnaAcomph.tb.c['data'].in_(datas)
        )
        result = __DB__.db_execute(query)
        return {"deletes":result.rowcount}

    @staticmethod
    def post_ena_acomph(ena_acomph:List[dict]):
        df = pd.DataFrame(ena_acomph)
        df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
        df['data'] = df['data'].dt.date
        EnaAcomph.delete_ena_acomph_by_dates(df['data'].unique().tolist())
        query = db.insert(EnaAcomph.tb).values(ena_acomph)
        result = __DB__.db_execute(query)
        return {"inserts":result.rowcount}
    
    @staticmethod
    def get_ena_acomph_by_granularidade_date_between(
        granularidade:str, data_inicial:datetime.date, data_final:datetime.date
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
    tb:db.Table = __DB__.getSchema('tb_ve_bacias')
    
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
