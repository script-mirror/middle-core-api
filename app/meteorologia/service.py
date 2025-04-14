import sqlalchemy as sa
import pandas as pd
from typing import List, Dict, Any

from app.core.database.wx_dbClass import db_mysql_master

__DB__ = db_mysql_master('db_meteorologia')

class EstacaoChuvosaObservada:
    # Define tables at class level - they'll be assigned in the methods since they depend on parameters
    tb_ec_norte = __DB__.getSchema('tb_chuva_observada_estacao_chuvosa_norte')
    tb_ec_sudeste = __DB__.getSchema('tb_chuva_observada_estacao_chuvosa')
    tb_cadastro_ec_norte = __DB__.getSchema('tb_cadastro_estacao_chuvosa_norte')
    tb_cadastro_ec_sudeste = __DB__.getSchema('tb_cadastro_estacao_chuvosa')
    tb_chuva_prevista_ec_norte = __DB__.getSchema('tb_chuva_prevista_estacao_chuvosa_norte')
    tb_chuva_prevista_ec_sudeste = __DB__.getSchema('tb_chuva_prevista_estacao_chuvosa')

    @staticmethod
    def get_chuva_observada(dt_ini_obs, dt_fim_obs, regiao):

        if regiao == 'norte':
            tb_ec = EstacaoChuvosaObservada.tb_ec_norte
            
        elif regiao == 'sudeste':
            tb_ec = EstacaoChuvosaObservada.tb_ec_sudeste

        dt_ini_obs = pd.to_datetime(dt_ini_obs, format="%Y-%m-%d")
        dt_fim_obs = pd.to_datetime(dt_fim_obs, format="%Y-%m-%d")

        select_cpc = sa.select(
            tb_ec.c.dt_observada,
            tb_ec.c.vl_chuva,
            ).where(
                tb_ec.c.dt_observada >= dt_ini_obs.strftime("%Y-%m-%d"),
                tb_ec.c.dt_observada <= dt_fim_obs.strftime("%Y-%m-%d"),
            )
                
        cpc_values = __DB__.db_execute(select_cpc).fetchall()
        df_cpc = pd.DataFrame(cpc_values, columns=["dt_observada", "vl_chuva"])

        return df_cpc.to_dict('records')
    
    @staticmethod
    def get_chuva_prevista_estacao_chuvosa(modelo: str, hr_rodada: int, dt_rodada: str, regiao: str):
        
        if regiao == 'norte':
            tb_cadastro_estacao_chuvosa = EstacaoChuvosaObservada.tb_cadastro_ec_norte
            tb_chuva_prevista_estacao_chuvosa = EstacaoChuvosaObservada.tb_chuva_prevista_ec_norte
        elif regiao == 'sudeste':
            tb_cadastro_estacao_chuvosa = EstacaoChuvosaObservada.tb_cadastro_ec_sudeste
            tb_chuva_prevista_estacao_chuvosa = EstacaoChuvosaObservada.tb_chuva_prevista_ec_sudeste

        select = sa.select(
            tb_cadastro_estacao_chuvosa.c.str_modelo,
            tb_cadastro_estacao_chuvosa.c.dt_rodada,
            tb_cadastro_estacao_chuvosa.c.hr_rodada,
            tb_cadastro_estacao_chuvosa.c.id,
        ).where(
            tb_cadastro_estacao_chuvosa.c.str_modelo==modelo
        ).where(
            tb_cadastro_estacao_chuvosa.c.dt_rodada==dt_rodada
        ).where(
            tb_cadastro_estacao_chuvosa.c.hr_rodada==hr_rodada
        )

        id = __DB__.db_execute(select).fetchall()[0][-1]
    
        select_values = sa.select(
            tb_chuva_prevista_estacao_chuvosa.c.dt_prevista,
            tb_chuva_prevista_estacao_chuvosa.c.vl_chuva,
        ).where(
            tb_chuva_prevista_estacao_chuvosa.c.id_cadastro==id
        )

        vl_chuva = __DB__.db_execute(select_values).fetchall()
        df_prev = pd.DataFrame(vl_chuva, columns=["dt_prevista", "vl_chuva"])
        df_prev['modelo'] = modelo
        df_prev['hr_rodada'] = hr_rodada
        df_prev['dt_rodada'] = dt_rodada

        return df_prev.to_dict('records')
    
class ClimatologiaChuva:

    @staticmethod
    def get_climatologia():

        __DB__ = db_mysql_master('db_meteorologia')
        tb_climatologia = __DB__.getSchema('tb_climatologia_bacias_merge')
        
        select = sa.select(
            tb_climatologia.c.str_bacia,
            tb_climatologia.c.time,
            tb_climatologia.c.climatologia,
        )
        
        vl_chuva = __DB__.db_execute(select).fetchall()
        df_climatologia = pd.DataFrame(vl_chuva, columns=["bacia", "time", "climatologia"])

        return df_climatologia.to_dict('records')