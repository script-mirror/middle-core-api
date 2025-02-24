from sys import path
import pdb
import sqlalchemy as sa
import pandas as pd
import numpy as np
import datetime


from app.core.database.wx_dbClass import db_mysql_master

prod = True

class tb_bacias:
    @staticmethod
    def get_bacias(divisao:str):
        __DB__ = db_mysql_master('db_ons')
        bacias = __DB__.getSchema('tb_bacias')

        query = sa.select(
          bacias.c['id_bacia'],
          bacias.c['str_bacia'],
        )
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['id','nome'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        if divisao == 'tb_chuva':
            df = df[~df['nome'].isin(['AMAZONAS','PARAGUAI','PARAÍBA_DO_SUL','SÃO_FRANCISCO','TOCANTINS'])]
        return df.to_dict('records')
    

class tb_submercado:
    @staticmethod
    def get_submercados():
        __DB__ = db_mysql_master('db_ons')
        submercado = __DB__.getSchema('tb_submercado')

        query = sa.select(
            submercado.c['cd_submercado'],
            submercado.c['str_submercado'],
            submercado.c['str_sigla']
        )
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['id', 'nome', 'str_sigla'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
        
if __name__ == "__main__":
    
    teste = ['tb_bacias',
    'tb_bacias',
    'tb_chuva',
    'tb_submercado',
    'tb_submercado']
    print(set(teste))
    pass