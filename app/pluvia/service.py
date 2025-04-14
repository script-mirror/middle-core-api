import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
from app.core.database.wx_dbClass import db_mysql_master
from app.ons import service as ons_service

__DB__ = db_mysql_master('db_pluvia')



class PluviaEna:
    tb:db.Table = __DB__.getSchema('tb_pluvia_ena')
    
    @staticmethod
    def query_ultimo_id_pluvia_df(dt_rodada, flag_pzerada):

        id_rodada_pluvia = __DB__.db_execute(
        db.select(db.func.max(PluviaEna.tb.c['ID_RODADA']))
            .where(
                db.and_(
                    PluviaEna.tb.c['DT_RODADA'] == dt_rodada,
                    PluviaEna.tb.c['STR_MAPA'] == "Prec. Zero" if flag_pzerada else PluviaEna.tb.c['STR_MAPA'] != "Prec. Zero",
                ))
        ).scalar()

        if not id_rodada_pluvia:
            print("Resultados do pluvia nao estao disponíveis!")
            return {"response":"Resultados do pluvia nao estao disponíveis!"}
        print(f"Utilizando ID do pluvia: {id_rodada_pluvia}")

        lista_bacias_interesses = {'SÃO FRANCISCO (NE)': 21}
        answer = __DB__.db_execute(
            db.select(
                PluviaEna.tb.c['DT_REFERENTE'],
                PluviaEna.tb.c['VL_ENA'],
                PluviaEna.tb.c['CD_BACIA'],
            ).where(
                db.and_(PluviaEna.tb.c['ID_RODADA'] == id_rodada_pluvia, PluviaEna.tb.c['CD_BACIA'] == 21)
            )
        )
        df_rodada_pluvia = pd.DataFrame(answer.fetchall(), columns= ['dt_referente','vl_ena','cd_bacia'])
        df_bacias = pd.DataFrame(ons_service.BaciasSegmentadas.get_bacias_segmentadas_by_cd_bacia(21),columns=['cd_bacia','cd_submercado'])

        df_rodada_pluvia = pd.merge(df_rodada_pluvia,df_bacias, on=['cd_bacia'])
        

        df_rodada_pluvia['str_submercado'] = df_rodada_pluvia.apply(lambda x: "SE" if x['cd_submercado'] == 1 else "S" if x['cd_submercado'] == 2 else "NE" if x['cd_submercado'] == 3 else "N",axis=1)
    
        df_rodada_pluvia['dt_referente'] = pd.to_datetime(df_rodada_pluvia['dt_referente'], format="%Y/%m/%d")
        return df_rodada_pluvia.to_dict('records')