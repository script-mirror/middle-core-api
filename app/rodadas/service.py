from app.core.utils.logger import logging
import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
import requests as r
from requests.exceptions import HTTPError
from typing import Optional, List
from fastapi import HTTPException
from app.core.config import settings

from .schema import (
    RodadaSmap,
    PesquisaPrevisaoChuva,
    ChuvaObsReq,
    ChuvaPrevisaoCriacao,
    ChuvaPrevisaoCriacaoMembro,
    SmapCreateDto,
    SmapReadDto
)

from app.core.utils import cache, date_util
from ..ons import service as ons_service
from app.core.utils.auth_token import get_access_token
from app.airflow import service as airflow_service
# from app.utils.airflow.airflow_service import trigger_dag_SMAP
from app.core.database.wx_dbClass import db_mysql_master

logger = logging.getLogger(__name__)

__DB__ = db_mysql_master('db_rodadas')


class CadastroRodadas:
    tb: db.Table = __DB__.getSchema('tb_cadastro_rodadas')

    @staticmethod
    def get_rodadas_por_dt(dt: datetime.date) -> List[dict]:
        dt = datetime.date.today() if dt is None else dt

        query = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['id_chuva'],
            CadastroRodadas.tb.c['id_smap'],
            CadastroRodadas.tb.c['id_previvaz'],
            CadastroRodadas.tb.c['id_prospec'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['fl_preliminar'],
            CadastroRodadas.tb.c['fl_pdp'],
            CadastroRodadas.tb.c['fl_psat'],
            CadastroRodadas.tb.c['fl_estudo'],
            CadastroRodadas.tb.c['dt_revisao']
        ).where(
            CadastroRodadas.tb.c['dt_rodada'] == dt
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'id',
                'id_chuva',
                'id_smap',
                'id_previvaz',
                'id_prospec',
                'dt_rodada',
                'hr_rodada',
                'str_modelo',
                'fl_preliminar',
                'fl_pdp',
                'fl_psat',
                'fl_estudo',
                'dt_revisao'])
        df['dt_rodada'] = df['dt_rodada'].astype(
            'datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')

    @staticmethod
    def get_rodadas_by_id(id_rodada: int):
        query = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['id_chuva'],
            CadastroRodadas.tb.c['id_smap'],
            CadastroRodadas.tb.c['id_previvaz'],
            CadastroRodadas.tb.c['id_prospec'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['fl_preliminar'],
            CadastroRodadas.tb.c['fl_pdp'],
            CadastroRodadas.tb.c['fl_psat'],
            CadastroRodadas.tb.c['fl_estudo'],
            CadastroRodadas.tb.c['dt_revisao']
        ).where(
            CadastroRodadas.tb.c['id'] == id_rodada
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'id',
                'id_chuva',
                'id_smap',
                'id_previvaz',
                'id_prospec',
                'dt_rodada',
                'hr_rodada',
                'str_modelo',
                'fl_preliminar',
                'fl_pdp',
                'fl_psat',
                'fl_estudo',
                'dt_revisao'])
        df['dt_rodada'] = df['dt_rodada'].astype(
            'datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        if df.empty:
            raise HTTPException(
                404, {
                    "erro": f"Nenhuma rodada encontrada com id {id_rodada}"})
        return df.to_dict('records')[0]

    @staticmethod
    def get_historico_rodadas_por_nome(nome_rodada: str) -> List[dict]:
        query = db.select(
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada']
        ).where(
            CadastroRodadas.tb.c['str_modelo'] == nome_rodada
        ).distinct()
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['modelo', 'dt_rodada', 'hr_rodada'])
        return df.to_dict('records')

    @staticmethod
    def get_rodadas_por_dt_hr_nome(
            dt: datetime.datetime,
            nome: str) -> List[dict]:
        dt = datetime.datetime(
            datetime.date.today().year,
            datetime.date.today().month,
            datetime.date.today().day,
            0) if dt is None else dt

        query = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['id_chuva'],
            CadastroRodadas.tb.c['id_smap'],
            CadastroRodadas.tb.c['id_previvaz'],
            CadastroRodadas.tb.c['id_prospec'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['fl_preliminar'],
            CadastroRodadas.tb.c['fl_pdp'],
            CadastroRodadas.tb.c['fl_psat'],
            CadastroRodadas.tb.c['fl_estudo'],
            CadastroRodadas.tb.c['dt_revisao']).where(
            db.and_(
                CadastroRodadas.tb.c['dt_rodada'] == dt.date(),
                CadastroRodadas.tb.c['hr_rodada'] == dt.hour,
                CadastroRodadas.tb.c['str_modelo'] == nome)).order_by(
                CadastroRodadas.tb.c['fl_preliminar'],
                CadastroRodadas.tb.c['fl_pdp'].desc(),
            CadastroRodadas.tb.c['fl_psat'].desc())

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'id',
                'id_chuva',
                'id_smap',
                'id_previvaz',
                'id_prospec',
                'dt_rodada',
                'hr_rodada',
                'str_modelo',
                'fl_preliminar',
                'fl_pdp',
                'fl_psat',
                'fl_estudo',
                'dt_revisao'])
        if df.empty:
            raise HTTPException(
                404, {
                    "erro": f"Nenhum modelo encontrado com nome "
                            f"{nome} e data de rodada {dt}"
                    })

        df['dt_rodada'] = df['dt_rodada'].astype(
            'datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')

    @staticmethod
    def info_rodadas(modelos: list, columns_data: list = []):
        if not columns_data:
            selected_columns = [
                column_name for column_name in CadastroRodadas.tb.columns]
        else:
            conditional_columns = [
                CadastroRodadas.tb.c['id_chuva'] if 'id_chuva' in columns_data
                else None,
                CadastroRodadas.tb.c['id_smap'] if 'id_smap' in columns_data
                else None,
            ]
            base_columns = [
                CadastroRodadas.tb.c['id'],
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']
            ]

            selected_columns = base_columns + \
                [col for col in conditional_columns if col is not None]

        order_priority = db.case(
            ((CadastroRodadas.tb.c['fl_pdp'] == 1) & (
                CadastroRodadas.tb.c['fl_preliminar'] == 0),
                1),
            ((CadastroRodadas.tb.c['fl_psat'] == 1) & (
                CadastroRodadas.tb.c['fl_preliminar'] == 0),
                2),
            ((CadastroRodadas.tb.c['fl_pdp'] == 0) & (
                CadastroRodadas.tb.c['fl_psat'] == 0) & (
                    CadastroRodadas.tb.c['fl_preliminar'] == 0),
             3),
            (CadastroRodadas.tb.c['fl_preliminar'] == 1,
             4),
            else_=None).label('order_priority')

        subquery_cadastro_rodadas = db.select(
            selected_columns +
            order_priority).where(
            db.tuple_(
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']).in_(modelos)) .order_by(
                db.desc(
                    CadastroRodadas.tb.c['dt_rodada']),
            db.asc(order_priority))

        rodadas_values = __DB__.db_execute(
            subquery_cadastro_rodadas).fetchall()
        return pd.DataFrame(
            rodadas_values,
            columns=[
                column_name.name for column_name in selected_columns] +
            ['priority'])

    @staticmethod
    def get_last_column_id(column_name: str):
        query_get_max_id_column = db.select(
            db.func.max(CadastroRodadas.tb.c[column_name]))
        max_id = __DB__.db_execute(query_get_max_id_column).scalar()
        return max_id

    @staticmethod
    def inserir_cadastro_rodadas(rodadas_values: list):
        query_update = CadastroRodadas.tb.insert().values(rodadas_values)

        n_value = __DB__.db_execute(query_update).rowcount

        logger.info(f"{n_value} Linhas inseridas na tb_cadastro_rodadas")

    @staticmethod
    def delete_por_id(id: int):
        query = CadastroRodadas.tb.delete(
        ).where(
            CadastroRodadas.tb.c['id'] == id
        )
        n_value = __DB__.db_execute(query).rowcount
        logger.info(f"{n_value} Linhas deletadas na cadastro rodadas")
        return None

    @staticmethod
    def delete_rodada_chuva_smap_por_id_rodada(id_rodada: int):
        q_select = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['id_chuva'],
            CadastroRodadas.tb.c['id_smap']
        ).where(
            CadastroRodadas.tb.c['id'] == id_rodada
        )

        result = __DB__.db_execute(q_select)
        try:
            ids = pd.DataFrame(
                result,
                columns=[
                    'id_rodada',
                    'id_chuva',
                    'id_smap']).to_dict('records')[0]
            Chuva.delete_por_id(ids['id_chuva'])
            Smap.delete_por_id(ids['id_smap'])
            CadastroRodadas.delete_por_id(ids['id_rodada'])
        except IndexError:
            logger.info(f'id rodada {id_rodada} nao existe.')

    def get_by_id_smap(id_smap:int) -> List[dict]:
        query_select = CadastroRodadas.tb.select(
            CadastroRodadas.tb.c['id_smap'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['fl_preliminar'],
            CadastroRodadas.tb.c['fl_pdp'],
            CadastroRodadas.tb.c['fl_psat']
            ).where(CadastroRodadas.tb.c['id_smap']==id_smap)
        result = __DB__.db_execute(query_select).fetchall()
        if not result:
            raise HTTPException(
                404, {
                    "erro": f"Nenhuma rodada encontrada com id_smap {id_smap}"})
        df = pd.DataFrame(result, columns=['id_smap', 
                                           'dt_rodada', 
                                           'hr_rodada', 
                                           'str_modelo', 
                                           'fl_preliminar', 
                                           'fl_pdp', 
                                           'fl_psat'])
        return df.to_dict('records')
        
        
    @staticmethod
    def update_id_smap(id_rodada:int, id_smap:int):
        id_rodada = int(id_rodada.iloc[0])
        query_update = CadastroRodadas.tb.update().values(
            id_smap=id_smap).where(
            CadastroRodadas.tb.c['id'] == id_rodada)
        n_value = __DB__.db_execute(query_update).rowcount
        logger.info(f"{n_value} Linhas atualizadas na tb_cadastro_rodadas")
        return None
    @staticmethod
    def upsert_rodada_smap(
        cenario:str,
        id_smap:int
    ):
        modelo_flags, dt_rodada ,hr_rodada, = cenario.split('_')
        modelo_splited = modelo_flags.split('.')
        
        str_modelo = modelo_splited[0]
        flags = modelo_splited[1:]
        flags = [flags] if isinstance(flags,str) else flags

        flag_preliminar,flag_psat,flag_pdp = 0,0,0

        for flag in flags:
            if flag == "PRELIMINAR" : 
                flag_preliminar = 1
                dt_rodada = (pd.to_datetime(dt_rodada) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            elif flag == "PDP": flag_pdp = 1 
            elif flag == "PSAT": flag_psat = 1
            if flag == "GPM": break


        rodada = pd.DataFrame(CadastroRodadas.get_rodadas_por_dt_hr_nome(datetime.datetime.strptime(f"{dt_rodada}T{hr_rodada}", "%Y-%m-%dT%H"), str_modelo))
        cadastro_rodada = ({
            "str_modelo":str_modelo,
            "dt_rodada": dt_rodada,
            "hr_rodada": hr_rodada,
            "fl_preliminar":flag_preliminar,
            "fl_pdp":flag_pdp,
            "fl_psat":flag_psat,
            "id_chuva":rodada['id_chuva'].values[0],
            "id_previvaz":rodada['id_previvaz'].values[0],
            "id_prospec":rodada['id_prospec'].values[0],
            "id_smap":id_smap,
        })
        if len(rodada)==1 and rodada['id_smap'][0] == None:
            CadastroRodadas.update_id_smap(rodada['id'], id_smap)
            return {"message": f"Rodada {rodada['id'].max()} atualizada com sucesso."}
        else:
            CadastroRodadas.inserir_cadastro_rodadas([cadastro_rodada])
            rodada = pd.DataFrame(CadastroRodadas.get_rodadas_por_dt_hr_nome(datetime.datetime.strptime(f"{dt_rodada}T{hr_rodada}", "%Y-%m-%dT%H"), str_modelo))
            return {"message": f"Rodada {rodada['id'].max()} inserida com sucesso."}
        

class Chuva:
    tb: db.Table = __DB__.getSchema('tb_chuva')

    @staticmethod
    def get_chuva_por_id(id_chuva: int):
        query = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            CadastroRodadas.tb.c['dt_revisao'],
            Chuva.tb.c['cd_subbacia'],
            Chuva.tb.c['dt_prevista'],
            Chuva.tb.c['vl_chuva']).where(
            db.and_(
                Chuva.tb.c['id'] == id_chuva)).join(
                CadastroRodadas.tb,
            CadastroRodadas.tb.c['id_chuva'] == Chuva.tb.c['id'])

        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'id_cadastro_rodada',
                'modelo',
                'dt_rodada',
                'hr_rodada',
                'dt_revisao',
                'id',
                'dt_prevista',
                'vl_chuva'])
        df['dia_semana'] = df['dt_prevista'].astype(
            'datetime64[ns]').dt.strftime('%A')
        df['dt_prevista'] = df['dt_prevista'].astype(
            'datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()
        df = df.sort_values('dt_prevista')

        dfs = [
            g for _,
            g in df.groupby(
                (pd.to_datetime(
                    df['dt_prevista']) +
                    pd.to_timedelta(
                    1,
                    unit='D')).dt.to_period('W'))]
        for i, item in enumerate(dfs):
            item['semana'] = i + 1
            dfs[i] = item
            pass
        df = pd.concat(dfs)
        return df.to_dict('records')

    @staticmethod
    def get_chuva_smap_ponderada_submercado(id_chuva):

        df_bacia = pd.DataFrame(
            ons_service.BaciasSegmentadas.get_bacias_segmentadas())[
            ['cd_bacia', 'str_bacia']]

        df_chuva = pd.DataFrame(
            Chuva.get_chuva_por_id(id_chuva))[
            [
                'id_cadastro_rodada',
                'modelo',
                'hr_rodada',
                'id',
                'dt_prevista',
                'vl_chuva',
                'dt_revisao',
                'dt_rodada']]
        df_chuva.rename(
            columns={
                'id': 'cd_subbacia',
                'id_cadastro_rodada': 'id',
                'modelo': 'str_modelo'},
            inplace=True)

        cds_subbacia = df_chuva['cd_subbacia'].unique()

        df_subbacia = pd.DataFrame(Subbacia.get_subbacia())[
            ['id', 'cd_bacia_mlt', 'nome_submercado']]
        df_subbacia = df_subbacia.rename(
            columns={
                'id': 'cd_subbacia',
                'nome_submercado': 'txt_submercado',
                'cd_bacia_mlt': 'cd_bacia'})
        df_subbacia = df_subbacia[df_subbacia['cd_subbacia'].isin(
            cds_subbacia)]

        df_chuva_concat = pd.merge(
            df_chuva,
            df_subbacia,
            on=['cd_subbacia'],
            how='inner')
        df_chuva_concat = pd.merge(df_chuva_concat, df_bacia, on=['cd_bacia'], how='inner')

        df_chuva_concat['dt_prevista'] = pd.to_datetime(
            df_chuva_concat['dt_prevista'])

        df_chuva_concat['dt_inicio_semana'] = df_chuva_concat[
            'dt_prevista'
        ].apply(
            lambda x: date_util.getLastSaturday(x)
        ).dt.strftime('%Y-%m-%d')

        dt_rodada = df_chuva['dt_rodada'].max()
        dt = date_util.getLastSaturday(dt_rodada)

        df_mlt = pd.DataFrame(ons_service.VeBacias.get_ve_bacias(dt))[
            ['cd_bacia', 'vl_mes', 'dt_inicio_semana', 'cd_revisao', 'mlt']]
        df_mlt = df_mlt.sort_values(['vl_mes', 'cd_revisao'], ascending=False)

        df_mlt['dt_inicio_semana'] = pd.to_datetime(
            df_mlt['dt_inicio_semana']).dt.strftime('%Y-%m-%d')
        df_mlt = df_mlt.drop_duplicates(
            ['dt_inicio_semana', 'cd_bacia'], keep='first')

        df_mlt = df_mlt.sort_values('dt_inicio_semana')

        df_concatenado = pd.merge(
            df_chuva_concat, df_mlt, on=[
                'dt_inicio_semana', 'cd_bacia'], how='inner')
        df_concatenado['chuvaxmlt'] = df_concatenado['vl_chuva'] * \
            df_concatenado['mlt']

        df_concatenado['txt_submercado'] = df_concatenado[
            'txt_submercado'
        ].apply(
            lambda x: 'SE' if x == 'Sudeste'
            else 'S' if x == 'Sul'
            else 'N' if x == 'Norte'
            else 'NE'
            )
        df = df_concatenado.groupby(["str_modelo",
                                     "id",
                                     'hr_rodada',
                                     "txt_submercado",
                                     'dt_prevista'])[['chuvaxmlt',
                                                      'mlt']].sum()
        df['chuva_pond'] = df['chuvaxmlt'] / df['mlt']
        df.reset_index(inplace=True)
        df.rename(
            columns={
                'str_modelo': 'modelo',
                'chuva_pond': 'vl_chuva',
                'txt_submercado': 'str_sigla'},
            inplace=True)
        df['dt_rodada'] = dt_rodada

        return df[["dt_rodada", "dt_prevista", "hr_rodada",
                   "modelo", "vl_chuva", "str_sigla"]].to_dict("records")

    @staticmethod
    def get_chuva_observada_ponderada_submercado(
            data_inicio: datetime.date, data_fim: datetime.date):
        db_ons = db_mysql_master('db_ons')
        db_ons.connect()

        tb_ve_bacias = db_ons.getSchema('tb_ve_bacias')
        tb_bacias_segmentadas = db_ons.getSchema('tb_bacias_segmentadas')

        # rodadas
        db_rodadas = db_mysql_master('db_rodadas')
        db_rodadas.connect()

        tb_chuva_obs = db_rodadas.getSchema('tb_chuva_obs')
        tb_subbacia = db_rodadas.getSchema('tb_subbacia')

        # query para pegar os nomes das subbacias
        query_subbacia = db.select(
            tb_subbacia.c.cd_subbacia,
            tb_subbacia.c.cd_bacia_mlt,
            tb_subbacia.c.txt_submercado)
        answer_subbacia = db_rodadas.db_execute(query_subbacia).fetchall()
        df_subbacia = pd.DataFrame(
            answer_subbacia,
            columns=[
                'cd_subbacia',
                'cd_bacia_mlt',
                'txt_submercado'])

        # query para pegar os nomes das bacias
        query_bacia = db.select(
            tb_bacias_segmentadas.c.cd_bacia,
            tb_bacias_segmentadas.c.str_bacia)
        answer_tb_bacias_segmentadas = db_ons.db_execute(
            query_bacia).fetchall()
        df_bacia = pd.DataFrame(
            answer_tb_bacias_segmentadas, columns=[
                'cd_bacia_mlt', 'str_bacia'])

        # concatenando dataframe valores de chuva e nomes subbacias
        df_bacia_subbacia_concat = pd.merge(
            df_subbacia, df_bacia, on=['cd_bacia_mlt'], how='inner')

        # query para pegar valores de chuva observada
        query_chuva_obs = db.select(
            tb_chuva_obs.c.cd_subbacia,
            tb_chuva_obs.c.dt_observado,
            tb_chuva_obs.c.vl_chuva).where(
            tb_chuva_obs.c.dt_observado.between(
                data_inicio,
                data_fim))

        answer_tb_chuva_obs = db_rodadas.db_execute(query_chuva_obs).fetchall()
        df_chuva_obs = pd.DataFrame(
            answer_tb_chuva_obs, columns=[
                'cd_subbacia', 'dt_observado', 'vl_chuva'])

        # concatenando dataframe valores de chuva e nomes subbacias
        df_chuva_concat = pd.merge(
            df_chuva_obs,
            df_bacia_subbacia_concat,
            on=['cd_subbacia'],
            how='inner')

        # pegando data de revisao que sempre é o sabado anterior ao dia
        # escolhido
        ultimoSabado = date_util.getLastSaturday(data_inicio)

        # query para transformar porcentagem mlt em valor MLT
        select_query = db.select(
            tb_ve_bacias.c.cd_bacia,
            tb_ve_bacias.c.vl_mes,
            tb_ve_bacias.c.dt_inicio_semana,
            tb_ve_bacias.c.cd_revisao,
            ((tb_ve_bacias.c.vl_ena *
              100) /
             db.func.nullif(
                tb_ve_bacias.c.vl_perc_mlt,
                0)).label('mlt')) .where(
            tb_ve_bacias.c.dt_inicio_semana >= ultimoSabado)
        answer_tb_ve_bacias = db_ons.db_execute(select_query).fetchall()
        # criando dataframe mlt ordenando pela data inicial da semana
        df_mlt = pd.DataFrame(
            answer_tb_ve_bacias,
            columns=[
                'cd_bacia_mlt',
                'vl_mes',
                'dt_inicio_semana',
                'cd_revisao',
                'mlt'])
        df_mlt = df_mlt.sort_values(['vl_mes', 'cd_revisao'], ascending=False)
        # Remover as linhas duplicadas na ColunaA, mantendo apenas a linha com
        # o valor máximo em ColunaB
        df_mlt['dt_inicio_semana'] = pd.to_datetime(
            df_mlt['dt_inicio_semana']).dt.strftime('%Y-%m-%d')
        df_mlt = df_mlt.drop_duplicates(
            ['dt_inicio_semana', 'cd_bacia_mlt'], keep='first')

        # concatenando valores de mlt, mltxchuva e valores de chuva em um unico
        # dataframe
        df_chuva_concat['dt_observado'] = pd.to_datetime(
            df_chuva_concat['dt_observado'])
        df_chuva_concat['dt_inicio_semana'] = df_chuva_concat[
            'dt_observado'
        ].apply(
            lambda x: date_util.getLastSaturday(x)).dt.strftime('%Y-%m-%d')
        df_concatenado = pd.merge(
            df_chuva_concat, df_mlt, on=[
                'dt_inicio_semana', 'cd_bacia_mlt'], how='inner')
        df_concatenado['chuvaxmlt'] = df_concatenado['vl_chuva'] * \
            df_concatenado['mlt']
        df_concatenado['txt_submercado'] = df_concatenado[
            'txt_submercado'
        ].apply(
            lambda x: 'SE' if x == 'Sudeste'
            else 'S' if x == 'Sul'
            else 'N' if x == 'Norte'
            else 'NE'
            )

        df_concatenado['dt_observado'] = pd.to_datetime(
            df_concatenado['dt_observado']).dt.strftime('%Y-%m-%d')
        df = df_concatenado.groupby(["txt_submercado", 'dt_observado'])[
            ['chuvaxmlt', 'mlt']].sum()
        df['chuva_pond'] = df['chuvaxmlt'] / df['mlt']
        df.reset_index(inplace=True)
        df = df[["dt_observado", "txt_submercado", "chuva_pond"]].rename(
            columns={'txt_submercado': 'submercado', 'chuva_pond': 'mm_chuva',
                     'dt_observado': 'data'})
        return df.to_dict("records")

    @staticmethod
    def export_chuva_observada_ponderada_submercado(
            data_inicio: datetime.date, data_fim: datetime.date):
        df = pd.DataFrame(
            Chuva.get_chuva_observada_ponderada_submercado(
                data_inicio, data_fim))
        df.rename(
            columns={
                'submercado': 'valorAgrupamento',
                'mm_chuva': 'valor',
                'data': 'dataReferente'},
            inplace=True)
        df['dataReferente'] = pd.to_datetime(
            df['dataReferente']).dt.strftime('%Y-%m-%dT00:00:00.000Z')
        df['valorAgrupamento'] = df['valorAgrupamento'].str.replace(' ', '')
        payload = {
            "dataRodada": f"{data_fim}T00:00:00.000Z",
            "dataFinal": f"{data_fim}T00:00:00.000Z",
            "mapType": "chuva",
            "idType": None,
            "modelo": "CHUVA-GPM",
            "priority": None,
            "grupo": "ONS",
            "rodada": "0",
            "viez": True,
            "membro": "0",
            "measuringUnit": "mm",
            "propagationBase": None,
            "generationProcess": None,
            "data": [
                {
                    "valoresMapa": df.to_dict("records"),
                    "agrupamento": "submercado"
                }
            ]
        }
        accessToken = get_access_token()
        api_url = f'{settings.API_URL}/map'

        res = r.post(api_url, verify=False, json=payload, headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {accessToken}'})

        logger.info(res.text)
        try:
            res.raise_for_status()
        except HTTPError as http_err:
            logger.error(f"HTTP error: {http_err}")
        except Exception as err:
            logger.error(f"Other error: {err}")
        else:
            if res.status_code == 201:
                logger.info("Chuva ponderada inserida")
            else:
                logger.warning("Erro ao tentar inserir chuva ponderada")
        return None

    @staticmethod
    def get_chuva_por_id_data_entre_granularidade(
        id_chuva: int,
        granularidade: str,
        dt_inicio_previsao: Optional[datetime.date] = None,
        dt_fim_previsao: Optional[datetime.date] = None,
        no_cache: Optional[bool] = False,
        atualizar: Optional[bool] = False
    ):

        if no_cache:
            df = pd.DataFrame(Chuva.get_chuva_por_id(id_chuva))[
                ["modelo", "dt_rodada", "hr_rodada", "id",
                 "dt_prevista", "vl_chuva", "dia_semana", "semana"]
            ]
        else:
            df = pd.DataFrame(cache.get_cached(Chuva.get_chuva_por_id,
                                               id_chuva, atualizar=atualizar))[
                ["modelo", "dt_rodada", "hr_rodada", "id",
                 "dt_prevista", "vl_chuva", "dia_semana", "semana"]
            ]

        if df.empty:
            return df
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime(
                '%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime(
                    '%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >=
                    dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id'])
        if granularidade == 'subbacia':
            df = df.rename(columns={'id': 'cd_subbacia'})
            return df.to_dict('records')
        df_subbacia = pd.DataFrame(Subbacia.get_subbacia())
        merged = df.merge(
            df_subbacia[['id', 'nome_bacia', 'nome_submercado']], on='id')
        if granularidade == 'bacia':
            df_bacia = pd.DataFrame(
                ons_service.tb_bacias.get_bacias('tb_chuva'))
            merged['nome_bacia'] = merged['nome_bacia'].str.upper()
            merged = merged.replace(
                'STA. MARIA DA VITÓRIA', 'SANTA MARIA VITORIA').replace(
                'SÃO FRANCI', 'SÃO FRANCISCO').replace(
                'JEQUITINHO', 'JEQUITINHONHA').replace(
                'PARANAPANE', 'PARANAPANEMA'
            )
            grouped = merged.groupby(['nome_bacia',
                                      'dt_prevista',
                                      'dia_semana',
                                      'semana',
                                      'hr_rodada',
                                      'dt_rodada',
                                      'modelo']).agg({'vl_chuva': 'mean'}
                                                     ).reset_index()
            grouped = grouped.rename(columns={'nome_bacia': 'nome'}).merge(
                df_bacia[['id', 'nome']], on='nome')
            grouped = grouped.drop(
                columns=['nome']).rename(
                columns={
                    'id': 'id_bacia'})
            return grouped.to_dict('records')

        if granularidade == 'submercado':
            df_submercado = pd.DataFrame(
                ons_service.tb_submercado.get_submercados())
            df_submercado['nome'] = df_submercado['nome'].str.capitalize()
            grouped = merged.groupby(['nome_submercado',
                                      'dt_prevista',
                                      'dia_semana',
                                      'semana',
                                      'hr_rodada',
                                      'dt_rodada',
                                      'modelo']).agg(
                                          {'vl_chuva': 'mean'}
                                                    ).reset_index()
            grouped = grouped.rename(columns={'nome_submercado': 'nome'}
                                     ).merge(
                df_submercado[['id', 'nome']], on='nome')
            grouped = grouped.drop(
                columns=['nome']).rename(
                columns={
                    'id': 'id_submercado'})
            return grouped.to_dict('records')

    @staticmethod
    def get_chuva_por_nome_modelo_data_entre_granularidade(
            nome_modelo,
            dt_hr_rodada,
            granularidade,
            dt_inicio_previsao,
            dt_fim_previsao,
            no_cache,
            atualizar):
        rodadas = CadastroRodadas.get_rodadas_por_dt_hr_nome(
            dt_hr_rodada, nome_modelo)
        return Chuva.get_chuva_por_id_data_entre_granularidade(
            rodadas[0]["id_chuva"],
            granularidade,
            dt_inicio_previsao,
            dt_fim_previsao,
            no_cache,
            atualizar)

    @staticmethod
    def get_previsao_chuva_modelos_combinados(
            query_obj: List[PesquisaPrevisaoChuva],
            granularidade: str,
            no_cache: Optional[bool] = False,
            atualizar: Optional[bool] = False):

        df = pd.DataFrame()
        for obj in query_obj:
            q = obj.dict()
            df = pd.concat([df, pd.DataFrame(
                Chuva.get_chuva_por_id_data_entre_granularidade(
                    q["id"],
                    granularidade,
                    q["dt_inicio"],
                    q["dt_fim"],
                    no_cache,
                    atualizar
                ))])
        return df.rename(columns={'cd_subbacia': 'id'}).to_dict('records')

    @staticmethod
    def export_rain(id_chuva: int):

        def get_data_grouped(id_chuva: int, granularidade: str):
            return pd.DataFrame(
                Chuva.get_chuva_por_id_data_entre_granularidade(
                    id_chuva, granularidade))

        df_map_grouped_by_subbacia = get_data_grouped(id_chuva, 'subbacia')
        df_map_grouped_by_bacia = get_data_grouped(id_chuva, 'bacia')
        df_map_grouped_by_submercado = get_data_grouped(id_chuva, 'submercado')

        df_subbacias = pd.DataFrame(Subbacia.get_subbacia())
        df_bacias = pd.DataFrame(ons_service.tb_bacias.get_bacias('tb_chuva'))

        df_subbacias_merged = pd.merge(
            df_map_grouped_by_subbacia,
            df_subbacias,
            left_on='cd_subbacia',
            right_on='id')
        df_bacias_merged = pd.merge(
            df_map_grouped_by_bacia,
            df_bacias,
            left_on='id_bacia',
            right_on='id')

        df_map_grouped_by_subbacia = df_subbacias_merged[[
            'modelo', 'dt_rodada', 'hr_rodada',
            'dt_prevista', 'nome', 'vl_chuva']]
        df_map_grouped_by_bacia = df_bacias_merged[[
            'modelo', 'dt_rodada', 'dt_prevista', 'nome', 'vl_chuva']]
        df_map_grouped_by_submercado = pd.DataFrame(
            Chuva.get_chuva_smap_ponderada_submercado(id_chuva))

        df_model_base = df_map_grouped_by_subbacia[[
            'modelo', 'dt_rodada', 'hr_rodada']].drop_duplicates()
        model_base = df_model_base.to_dict('records')[0]

        data_rodada_date = datetime.datetime.combine(
            model_base['dt_rodada'], datetime.time(0))
        data_rodada_str = data_rodada_date.isoformat()
        data_rodada_str = f'{data_rodada_str}.000Z'
        data_final = None
        modelo = model_base['modelo']
        grupo = "ONS" if 'ons' in model_base["modelo"].lower() else "RZ"
        viez = False if 'remvies' in model_base["modelo"].lower() else True

        data = []
        agrupamentos = {
            'subbacia': {'valoresMapa': [], 'agrupamento': 'subbacia'},
            'bacia': {'valoresMapa': [], 'agrupamento': 'bacia'},
            'submercado': {'valoresMapa': [], 'agrupamento': 'submercado'}
        }

        for tipo, values in [
            ('subbacia', df_map_grouped_by_subbacia.to_dict('records')),
            ('bacia', df_map_grouped_by_bacia.to_dict('records')),
            ('submercado', df_map_grouped_by_submercado.to_dict('records'))
        ]:

            for value in values:

                valorAgrupamento = value['nome'] if tipo in [
                    'subbacia', 'bacia'] else value['str_sigla']
                try:
                    data_referente_date = datetime.datetime.strptime(
                        value['dt_prevista'], '%Y-%m-%d')
                except Exception:
                    data_referente_date = datetime.datetime.strptime(
                        f"{value['dt_prevista']}", '%Y-%m-%d %H:%M:%S')

                agrupamentos[tipo]['valoresMapa'].append({
                    "valor":
                        value['vl_chuva'],
                    "dataReferente":
                        f'{data_referente_date.date()}T00:00:00.000Z',
                    "valorAgrupamento":
                        valorAgrupamento
                })

                if data_final is None or data_referente_date > data_final:
                    data_final = data_referente_date
                    data_final_str = f'{data_final.date()}T00:00:00.000Z'

        data = [agrup for agrup in agrupamentos.values()
                if agrup['valoresMapa']]

        nome_modelo = model_base['modelo'].replace(
            '-ONS', ''
        ).replace(
            '-RZ', ''
        ).replace(
            '-REMVIES', ''
        ).replace(' ', '')
        body = {
            "dataRodada": data_rodada_str,
            "dataFinal": data_final_str,
            "mapType": "chuva",
            "idType": str(id_chuva),
            "modelo": nome_modelo,
            "priority": None,
            "grupo": grupo,
            "rodada": str(model_base['hr_rodada']),
            "viez": viez,
            "membro": "0",
            "measuringUnit": "mm",
            "propagationBase": None,
            "generationProcess": None,
            "data": data
        }

        accessToken = get_access_token()
        api_url = f'{settings.API_URL}/map'

        res = r.post(
            api_url,
            verify=False,
            json=body,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {accessToken}'})

        try:
            res.raise_for_status()
        except HTTPError as http_err:
            logger.error(f"HTTP error: {http_err}")
        except Exception as err:
            logger.error(f"Other error: {err}")
        else:
            if res.status_code == 201:
                logger.info(
                    f"Modelo {modelo} do dia {data_rodada_str} "
                    f"inserido no endereco ${api_url}")
                return res.json()['_id']
            else:
                logger.warning(
                    f"Erro ao tentar inserir o modelo {modelo} do dia "
                    "{data_rodada_str} no endereco ${api_url}")

    @staticmethod
    def post_chuva_modelo_combinados(
            chuva_prev: List[ChuvaPrevisaoCriacao],
            rodar_smap: bool,
            prev_estendida: bool) -> None:

        prevs: List[dict] = []
        for prev in chuva_prev:
            prevs.append(prev.model_dump())

        try:
            id_rodada = CadastroRodadas.get_rodadas_por_dt_hr_nome(
                prevs[0]['dt_rodada'], prevs[0]['modelo'])[0]['id']
            CadastroRodadas.delete_rodada_chuva_smap_por_id_rodada(id_rodada)
        except HTTPException:
            logger.info("Modelo nao encontrado.")

        modelo = (
            prevs[0]['modelo'],
            prevs[0]['dt_rodada'].hour,
            prevs[0]['dt_rodada'].date())

        for i in range(len(prevs)):
            prevs[i]['dt_rodada'] = prevs[i]['dt_rodada'].date()

        df = pd.DataFrame(prevs)
        df['cenario'] = f'{modelo[0]}_{modelo[1]}_{modelo[2]}'

        [id_chuva, dt_rodada, hr_rodada, str_modelo] = Chuva.inserir_chuva_modelos(
            df, prev_estendida
        )

        id_dataviz_chuva = Chuva.export_rain(id_chuva)

        if rodar_smap:
            Smap.trigger_rodada_smap(
                RodadaSmap.model_validate(
                    {
                        'dt_rodada': datetime.datetime.strptime(
                            dt_rodada,
                            '%Y-%m-%d'),
                        'hr_rodada': hr_rodada,
                        'str_modelo': str_modelo,
                        'id_dataviz_chuva': id_dataviz_chuva,
                        'prev_estendida': prev_estendida}))

        return None

    @staticmethod
    def inserir_prev_chuva(df_prev_vazao_out: pd.DataFrame):
        values_chuva = df_prev_vazao_out[[
            'id_chuva', 'cd_subbacia', 'dt_prevista', 'vl_chuva']].values.tolist()
        query_insert = Chuva.tb.insert().values(values_chuva)
        n_value = __DB__.db_execute(query_insert).rowcount
        logger.info(f"{n_value} Linhas inseridas na Chuva")

    @staticmethod
    def inserir_chuva_modelos(
            df_prev_chuva_out: pd.DataFrame,
            prev_estendida: bool):
        df_info_subbacias = Subbacia.info_subbacias()
        df_chuva_final = pd.merge(
            df_info_subbacias[['cd_subbacia', 'vl_lon', 'vl_lat']], df_prev_chuva_out)
        df_prev_chuva = df_chuva_final.drop(['vl_lat', 'vl_lon'], axis=1)
        # df_prev_chuva = df_chuva_final.melt(id_vars=['cd_subbacia','cenario', 'dt_prevista'] , value_vars='vl_chuva')

        """
        REMOVENDO CadastroRodadas.info_rodadas e retornando dataframe vazio diretamente
        """
        # df_info_rodadas = CadastroRodadas.info_rodadas(modelos)

        df_info_rodadas = pd.DataFrame(
            columns=[
                'str_modelo',
                'dt_rodada',
                'hr_rodada'])

        new_chuva_id = CadastroRodadas.get_last_column_id('id_chuva') + 1
        insert_cadastro_values = []
        for cenario in df_prev_chuva['cenario'].unique():

            str_modelo, hr_rodada, dt_rodada = cenario.split('_')

            mask_id_chuva = \
                (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & \
                (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & \
                (df_info_rodadas['hr_rodada'] == int(hr_rodada))

            if df_info_rodadas[mask_id_chuva].empty:
                insert_cadastro_values += [None,
                                           new_chuva_id,
                                           None,
                                           None,
                                           None,
                                           dt_rodada,
                                           int(hr_rodada),
                                           f'{str_modelo}'.upper(),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None],
                df_prev_chuva.loc[df_prev_chuva['cenario']
                                  == cenario, 'id_chuva'] = new_chuva_id

            else:
                df_info_rodadas['id_chuva'] = df_info_rodadas['id_chuva'].astype(
                    str).str.replace('nan', 'None')
                id_chuva = df_info_rodadas[mask_id_chuva]['id_chuva'].unique()[
                    0]
                logger.info(
                    f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada com id_chuva: {id_chuva}")

                if id_chuva != 'None':
                    df_prev_chuva.loc[df_prev_chuva['cenario']
                                      == cenario, 'id_chuva'] = id_chuva
                else:
                    logger.info(
                        f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada porem sem id_chuva, será cadastrado com id_chuva: {new_chuva_id}")
                    df_prev_chuva.loc[df_prev_chuva['cenario']
                                      == cenario, 'id_chuva'] = new_chuva_id

            if insert_cadastro_values:
                CadastroRodadas.inserir_cadastro_rodadas(
                    insert_cadastro_values)
            Chuva.inserir_prev_chuva(df_prev_chuva.round(2))

            return [new_chuva_id, dt_rodada, hr_rodada, str_modelo]

    @staticmethod
    def delete_por_id(id: int):
        query = Chuva.tb.delete(
        ).where(
            Chuva.tb.c['id'] == id
        )
        n_value = __DB__.db_execute(query).rowcount
        logger.info(f"{n_value} Linhas deletadas na Chuva")
        return None


class ChuvaMembro:
    tb: db.Table = __DB__.getSchema('tb_chuva_membro')

    @staticmethod
    def post_chuva_membro(
            chuva_prev: List[ChuvaPrevisaoCriacaoMembro],
            inserir_ensemble: bool,
            rodar_smap: bool) -> None:
        records = [obj.model_dump() for obj in chuva_prev]
        df = pd.DataFrame(records)
        # dt_hr_rodada:datetime.datetime = df[["dt_hr_rodada"]].drop_duplicates().to_dict("records")[0]["dt_hr_rodada"].to_pydatetime()
        # modelo: str = df[["modelo"]].drop_duplicates().to_dict("records")[0]["modelo"]
        # df_membro_modelo["id_rodada"] = CadastroRodadas.get_rodadas_por_dt_hr_nome(dt_hr_rodada, modelo)[0]["id"]
        df['dt_hr_rodada'] = df['dt_hr_rodada'].apply(
            lambda x: x.replace(minute=0, second=0, microsecond=0))
        df = df.rename(columns={"membro": "nome"})
        df_membro_modelo = df[["nome", "dt_hr_rodada", "modelo", "peso"]].drop_duplicates([
                                                                                          'nome', 'dt_hr_rodada'])

        df_membro_modelo = pd.DataFrame(
            MembrosModelo.inserir(
                df_membro_modelo.to_dict("records")))
        df = df.merge(df_membro_modelo, on="nome").rename(columns={"id": "id_membro_modelo"})[
            ["id_membro_modelo", "vl_chuva", "cd_subbacia", "dt_prevista"]]

        body = df.to_dict("records")
        ChuvaMembro.inserir(body)
        if inserir_ensemble:
            ChuvaMembro.media_membros(
                chuva_prev[0].dt_hr_rodada.replace(
                    minute=0,
                    second=0,
                    microsecond=0),
                chuva_prev[0].modelo,
                inserir=inserir_ensemble,
                rodar_smap=rodar_smap)
        return None

    @staticmethod
    def inserir(body: List[dict]):
        id_membro_modelo = []
        cd_subbacia = []
        df_delete = pd.DataFrame(body)
        id_membro_modelo = df_delete["id_membro_modelo"].unique().tolist()
        cd_subbacia = df_delete["cd_subbacia"].unique().tolist()
        
        search_params = (
            ChuvaMembro.tb.c["id_membro_modelo"].in_(id_membro_modelo),
            ChuvaMembro.tb.c["cd_subbacia"].in_(cd_subbacia),
            ChuvaMembro.tb.c["dt_prevista"].between(df_delete['dt_prevista'].min(), df_delete['dt_prevista'].max())
        )
        q_delete = ChuvaMembro.tb.delete().where(db.and_(
            *search_params
        ))
        
        linhas_delete = __DB__.db_execute(q_delete).rowcount
        logger.info(f"{linhas_delete} linhas inseridas chuva membro")

        query = ChuvaMembro.tb.insert().values(body)
        linhas_insert = __DB__.db_execute(query).rowcount
        logger.info(f"{linhas_insert} linhas inseridas chuva membro")

    @staticmethod
    def media_membros(
            dt_hr_rodada: datetime.datetime,
            modelo: str,
            inserir: bool = False,
            prev_estendida: bool = False,
            rodar_smap: bool = False) -> None:
        q_select = db.select(
            ChuvaMembro.tb.c['cd_subbacia'],
            ChuvaMembro.tb.c['dt_prevista'],
            ChuvaMembro.tb.c['vl_chuva']).join(
            MembrosModelo.tb,
            MembrosModelo.tb.c['id'] == ChuvaMembro.tb.c['id_membro_modelo']).where(
            db.and_(
                MembrosModelo.tb.c['dt_hr_rodada'] == dt_hr_rodada,
                MembrosModelo.tb.c['modelo'] == modelo))
        result = __DB__.db_execute(q_select)
        df = pd.DataFrame(
            result,
            columns=[
                'cd_subbacia',
                'dt_prevista',
                'vl_chuva'])
        df = df.groupby(['cd_subbacia', 'dt_prevista']).mean().reset_index()
        df['modelo'] = modelo
        df['dt_rodada'] = dt_hr_rodada
        df['dt_rodada'] = pd.Series(
            df['dt_rodada'].dt.to_pydatetime(), dtype=object)
        if inserir:
            Chuva.post_chuva_modelo_combinados([ChuvaPrevisaoCriacao.model_validate(
                x) for x in df.to_dict('records')], rodar_smap, prev_estendida)

    @staticmethod
    def get_chuva_por_nome_modelo_dt_hr(nome_modelo, dt_hr_rodada):
        query = db.select(
            MembrosModelo.tb.c['nome'],
            MembrosModelo.tb.c['modelo'],
            MembrosModelo.tb.c['dt_hr_rodada'],
            ChuvaMembro.tb.c['cd_subbacia'],
            ChuvaMembro.tb.c['dt_prevista'],
            ChuvaMembro.tb.c['vl_chuva']).where(
            db.and_(
                MembrosModelo.tb.c['modelo'] == nome_modelo,
                MembrosModelo.tb.c['dt_hr_rodada'] == dt_hr_rodada)).join(
                MembrosModelo.tb,
            MembrosModelo.tb.c['id'] == ChuvaMembro.tb.c['id_membro_modelo'])
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                'membro',
                'modelo',
                'dt_hr_rodada',
                'id',
                'dt_prevista',
                'vl_chuva'])
        df['dia_semana'] = df['dt_prevista'].astype(
            'datetime64[ns]').dt.strftime('%A')
        df['dt_prevista'] = df['dt_prevista'].astype(
            'datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()
        df = df.sort_values('dt_prevista')

        dfs = [
            g for _,
            g in df.groupby(
                (pd.to_datetime(
                    df['dt_prevista']) +
                    pd.to_timedelta(
                    1,
                    unit='D')).dt.to_period('W'))]
        for i, item in enumerate(dfs):
            item['semana'] = i + 1
            dfs[i] = item
            pass
        df = pd.concat(dfs)
        return df.to_dict('records')

    @staticmethod
    def get_chuva_por_nome_modelo_data_entre_granularidade(
            nome_modelo,
            dt_hr_rodada,
            granularidade,
            dt_inicio_previsao,
            dt_fim_previsao,
            no_cache,
            atualizar):

        if no_cache:
            df = pd.DataFrame(
                ChuvaMembro.get_chuva_por_nome_modelo_dt_hr(
                    nome_modelo, dt_hr_rodada))
        else:
            df = pd.DataFrame(
                cache.get_cached(
                    ChuvaMembro.get_chuva_por_nome_modelo_dt_hr,
                    nome_modelo,
                    dt_hr_rodada,
                    atualizar=atualizar))
        if df.empty:
            return list()
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime(
                '%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime('%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >=
                    dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id', 'membro'])
        if granularidade == 'subbacia':
            df.rename(columns={'id': 'cd_subbacia'}, inplace=True)
            return df.to_dict('records')


class Subbacia:
    tb: db.Table = __DB__.getSchema('tb_subbacia')

    @staticmethod
    def get_subbacia():
        query = db.select(
            Subbacia.tb.c['cd_subbacia'],
            Subbacia.tb.c['txt_nome_subbacia'],
            Subbacia.tb.c['txt_submercado'],
            Subbacia.tb.c['txt_bacia'],
            Subbacia.tb.c['vl_lat'],
            Subbacia.tb.c['vl_lon'],
            Subbacia.tb.c['txt_nome_smap'],
            Subbacia.tb.c['txt_pasta_contorno'],
            Subbacia.tb.c['cd_bacia_mlt'],
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(
            result,
            columns=[
                'id',
                'nome',
                'nome_submercado',
                'nome_bacia',
                'vl_lat',
                'vl_lon',
                'nome_smap',
                'pasta_contorno',
                'cd_bacia_mlt'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')

    @staticmethod
    def get_bacias():
        query = db.select(
            db.distinct(Subbacia.tb.c['txt_bacia'])
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, '')

    @staticmethod
    def info_subbacias():
        query = db.select(
            Subbacia.tb.c['cd_subbacia'],
            Subbacia.tb.c['vl_lon'],
            Subbacia.tb.c['vl_lat'],
            Subbacia.tb.c['txt_nome_subbacia'])
        answer_tb_subbacia = __DB__.db_execute(query)

        df_subbac = pd.DataFrame(
            answer_tb_subbacia,
            columns=[
                'cd_subbacia',
                'vl_lon',
                'vl_lat',
                'nome'])
        df_subbac['nome'] = df_subbac['nome'].str.lower()
        return df_subbac


class Smap:
    tb: db.Table = __DB__.getSchema('tb_smap')

    @staticmethod
    def get_last_id_smap() -> int:
        query = db.select(
            db.func.max(Smap.tb.c['id'])
        )
        result = __DB__.db_execute(query).scalar()
        return result if result is not None else 0
    @staticmethod
    def create(body:List[SmapCreateDto]) -> List[SmapReadDto]:
        id_smap = Smap.get_last_id_smap()+1
        df = pd.DataFrame([obj.model_dump() for obj in body])
        CadastroRodadas.upsert_rodada_smap(df['cenario'].unique()[0], id_smap)
        df['id'] = id_smap
        query = Smap.tb.insert().values(df.drop(columns=['cenario']).to_dict('records'))
        n_value = __DB__.db_execute(query).rowcount
        logger.info(f"{n_value} Linhas inseridas na tb_smap")
        return Smap.get_vazao_smap_by_id(id_smap)
        

    @staticmethod
    def trigger_rodada_smap(rodada: RodadaSmap):
        momento_req: datetime.datetime = datetime.datetime.now()
        # trigger_dag_SMAP(rodada.dt_rodada, [rodada.str_modelo], rodada.hr_rodada, momento_req)
        airflow_service.trigger_airflow_dag(
            dag_id="PREV_SMAP",
            json_produtos={
                "modelos": [[
                    rodada.str_modelo,
                    rodada.hr_rodada,
                    rodada.dt_rodada.strftime("%Y-%m-%d"),
                ]
                ],
                "prev_estendida": rodada.prev_estendida,
                "id_dataviz_chuva": rodada.id_dataviz_chuva
            },
            momento_req=momento_req

        )

        return None

    @staticmethod
    def get_vazao_smap_by_id(id_smap: int):
        query = db.select(
            Smap.tb.c['id'],
            Smap.tb.c['cd_posto'],
            Smap.tb.c['dt_prevista'],
            Smap.tb.c['vl_vazao_vna'],
            Smap.tb.c['vl_vazao_prevs']

        ).where(
            Smap.tb.c['id'] == id_smap
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(
            result,
            columns=[
                'id',
                'cd_posto',
                'dt_prevista',
                'vl_vazao_vna',
                'vl_vazao_prevs'])
        vazao_dto = [SmapReadDto.model_validate(x) for x in df.to_dict('records')]
        
        return vazao_dto

    @staticmethod
    def delete_por_id(id: int):
        query = Smap.tb.delete(
        ).where(
            Smap.tb.c['id'] == id
        )
        n_value = __DB__.db_execute(query).rowcount
        logger.info(f"{n_value} Linhas deletadas tb smap")
        return None


class MembrosModelo:
    tb: db.Table = __DB__.getSchema('tb_membro_modelo')

    @staticmethod
    def inserir(body: List[dict]) -> List[dict]:
        df_delete = pd.DataFrame(body)
        
        search_params = (MembrosModelo.tb.c["dt_hr_rodada"].in_(df_delete["dt_hr_rodada"].unique().tolist()),
                         MembrosModelo.tb.c["nome"].in_(df_delete["nome"].unique().tolist()),
                         MembrosModelo.tb.c["modelo"].in_(df_delete["modelo"].unique().tolist()))
        q_delete = MembrosModelo.tb.delete().where(db.and_(
            *search_params
        ))
        linhas_delete = __DB__.db_execute(q_delete, debug=f"{df_delete['dt_hr_rodada'].unique().tolist()}\n{df_delete['nome'].unique().tolist()}\n{df_delete['modelo'].unique().tolist()}").rowcount
        logger.info(f'{linhas_delete} linha(s) deletada(s) tb membro modelo')
        q_insert = MembrosModelo.tb.insert().values(body)
        linhas_insert = __DB__.db_execute(q_insert).rowcount
        logger.info(f'{linhas_insert} linha(s) inserida(s) tb membro modelo')
        q_select = db.select(
            MembrosModelo.tb.c["id"],
            MembrosModelo.tb.c["dt_hr_rodada"],
            MembrosModelo.tb.c["nome"],
            MembrosModelo.tb.c["modelo"]
        ).where(db.and_(
                *search_params
                )
                )
        result = __DB__.db_execute(q_select).fetchall()
        df = pd.DataFrame(
            result,
            columns=[
                "id",
                "dt_hr_rodada",
                "nome",
                "modelo"])
        return df.to_dict("records")

    @staticmethod
    def get_chuva_por_id_data_entre_granularidade(
            id_chuva: int,
            granularidade: str,
            dt_inicio_previsao: Optional[datetime.date] = None,
            dt_fim_previsao: Optional[datetime.date] = None,
            no_cache: Optional[bool] = False,
            atualizar: Optional[bool] = False):

        if no_cache:
            df = pd.DataFrame(
                Chuva.get_chuva_por_id(id_chuva))[
                [
                    "modelo",
                    "dt_rodada",
                    "hr_rodada",
                    "id",
                    "dt_prevista",
                    "vl_chuva",
                    "dia_semana",
                    "semana"]]
        else:
            df = pd.DataFrame(
                cache.get_cached(
                    Chuva.get_chuva_por_id,
                    id_chuva,
                    atualizar=atualizar))[
                [
                    "modelo",
                    "dt_rodada",
                    "hr_rodada",
                    "id",
                    "dt_prevista",
                    "vl_chuva",
                    "dia_semana",
                    "semana"]]
        if df.empty:
            return df
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime(
                '%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime('%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >=
                    dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id'])
        if granularidade == 'subbacia':
            df.rename(columns={'id': 'cd_subbacia'}, inplace=True)
            return df.to_dict('records')


class ChuvaObs:
    tb: db.Table = __DB__.getSchema('tb_chuva_obs')

    @staticmethod
    def post_chuva_obs(
        chuva_obs: List[ChuvaObsReq]
    ):
        chuva_obs = [c.model_dump() for c in chuva_obs]
        df = pd.DataFrame(chuva_obs)
        df['dt_observado'].to_list()

        query_delete = ChuvaObs.tb.delete(
        ).where(db.and_(
                ChuvaObs.tb.c['dt_observado'] == chuva_obs[0]['dt_observado']
                ))
        rows_delete = __DB__.db_execute(query_delete).rowcount
        logger.info(f'{rows_delete} linha(s) deletada(s)')
        query_insert = ChuvaObs.tb.insert(
        ).values(
            df.to_dict('records')
        )
        rows_insert = __DB__.db_execute(query_insert).rowcount
        logger.info(f'{rows_insert} linha(s) inserida(s)')

    @staticmethod
    def get_chuva_observada_por_data(
        dt_observado: datetime.date
    ):
        query_select = db.select(
            ChuvaObs.tb.c['cd_subbacia'],
            ChuvaObs.tb.c['dt_observado'],
            ChuvaObs.tb.c['vl_chuva']
        ).where(
            ChuvaObs.tb.c['dt_observado'] == dt_observado
        )
        result = __DB__.db_execute(query_select)
        df = pd.DataFrame(
            result,
            columns=[
                'cd_subbacia',
                'dt_observado',
                'vl_chuva'])
        return df.to_dict('records')

    @staticmethod
    def get_chuva_observada_range_datas(
        dt_ini: datetime.date,
        dt_fim: datetime.date
    ):
        query_select = db.select(
            ChuvaObs.tb.c['cd_subbacia'],
            ChuvaObs.tb.c['dt_observado'],
            ChuvaObs.tb.c['vl_chuva'],
        ).where(
            ChuvaObs.tb.c['dt_observado'] >= dt_ini,
            ChuvaObs.tb.c['dt_observado'] <= dt_fim
        )

        result = __DB__.db_execute(query_select, True)
        df = pd.DataFrame(
            result,
            columns=[
                'cd_subbacia',
                'dt_observado',
                'vl_chuva'])
        df['dt_observado'] = pd.to_datetime(df['dt_observado'].values)
        df = df.sort_values(by='dt_observado')
        return df.to_dict('records')


class ChuvaObsPsat:
    tb: db.Table = __DB__.getSchema('tb_chuva_psat')

    @staticmethod
    def post_chuva_obs_psat(
        chuva_obs: List[ChuvaObsReq]
    ):
        chuva_obs = [c.model_dump() for c in chuva_obs]
        df = pd.DataFrame(chuva_obs)
        df['dt_observado'].to_list()

        query_delete = ChuvaObsPsat.tb.delete().where(db.and_(
            ChuvaObsPsat.tb.c['dt_ini_observado'] == chuva_obs[0]['dt_observado']))
        rows_delete = __DB__.db_execute(query_delete).rowcount
        logger.info(f'{rows_delete} linha(s) deletada(s)')
        query_insert = ChuvaObsPsat.tb.insert(
        ).values(
            df.to_dict('records')
        )
        rows_insert = __DB__.db_execute(query_insert).rowcount
        logger.info(f'{rows_insert} linha(s) inserida(s)')

    @staticmethod
    def get_chuva_observada_psat_por_data(
        dt_observado: datetime.date
    ):
        query_select = db.select(
            ChuvaObsPsat.tb.c['cd_subbacia'],
            ChuvaObsPsat.tb.c['dt_ini_observado'],
            ChuvaObsPsat.tb.c['vl_chuva']
        ).where(
            ChuvaObsPsat.tb.c['dt_ini_observado'] == dt_observado
        )
        result = __DB__.db_execute(query_select)
        df = pd.DataFrame(
            result,
            columns=[
                'cd_subbacia',
                'dt_observado',
                'vl_chuva'])
        return df.to_dict('records')


class VazoesObs:
    tb: db.Table = __DB__.getSchema('tb_vazoes_obs')

    @staticmethod
    def get_vazao_observada_por_data_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date
    ):
        query = db.select(
            VazoesObs.tb.c["txt_subbacia"],
            VazoesObs.tb.c["cd_estacao"],
            VazoesObs.tb.c["txt_tipo_vaz"],
            VazoesObs.tb.c["dt_referente"],
            VazoesObs.tb.c["vl_vaz"]
        ).where(
            db.and_(
                VazoesObs.tb.c['dt_referente'] >= data_inicio,
                VazoesObs.tb.c['dt_referente'] <= data_fim
            )
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(
            result,
            columns=[
            "txt_subbacia",
            "cd_estacao",
            "txt_tipo_vaz",
            "dt_referente",
            "vl_vaz"])
        return df.to_dict('records')

if __name__ == '__main__':
    ChuvaMembro.media_membros(
        datetime.datetime(
            2024, 10, 24, 12, 0, 0), 'string')
    # Smap.enviar_email_status_smap(False, datetime.datetime.now(), "https://tradingenergiarz.com/airflow/dags/ONS_DADOS_ABERTOS/grid?tab=graph&dag_run_id=scheduled__2024-09-17T20%3A00%3A00%2B00%3A00")
    # CadastroRodadas.get_rodadas_por_dt(datetime.date.today())
    # teste = MembrosModeloSchema()

    # MembrosModelo.inserir()
    # Chuva.get_chuva_por_id_data_entre_granularidade(11438, datetime.date.today(),datetime.date.today()+datetime.timedelta(days=15), 'bacia')
    pass
