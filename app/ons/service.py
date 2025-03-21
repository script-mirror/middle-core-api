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



    
class tb_bacias_segmentadas:
    tb:db.Table = __DB__.getSchema('tb_bacias_segmentadas')

    @staticmethod
    def get_bacias_segmentadas():
        query = db.select(
          tb_bacias_segmentadas.tb.c['cd_bacia'],
          tb_bacias_segmentadas.tb.c['str_bacia'],
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['cd_bacia','str_bacia'])
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
            Acomph.tb.c.cd_posto,
            Acomph.tb.c.dt_referente,
            Acomph.tb.c.vl_vaz_inc_conso,
            Acomph.tb.c.vl_vaz_nat_conso,
            Acomph.tb.c.dt_acomph,
            db.func.row_number().over(
                partition_by=[Acomph.tb.c.cd_posto, Acomph.tb.c.dt_referente],
                order_by=Acomph.tb.c.dt_acomph.desc()
            ).label('row_number')
        ).where(
            Acomph.tb.c.dt_referente >= data
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
    
if __name__ == "__main__":
    
    teste = ['tb_bacias',
    'tb_bacias',
    'tb_chuva',
    'tb_submercado',
    'tb_submercado']
    print(set(teste))
    pass