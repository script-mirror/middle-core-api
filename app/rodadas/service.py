from sys import path
import pdb
from venv import logger
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
import requests as r
from requests.exceptions import HTTPError
from typing import Optional, List
from fastapi import HTTPException
from app.core.config import settings

from .schema import *

from app.core.utils import cache
from ..ons import service as ons_service
from app.core.utils.graphs import get_access_token
from app.airflow import service as airflow_service 
# from app.utils.airflow.airflow_service import trigger_dag_SMAP
from app.core.database.wx_dbClass import db_mysql_master


prod = True

__DB__ = db_mysql_master('db_rodadas')
class CadastroRodadas:
    tb:db.Table = __DB__.getSchema('tb_cadastro_rodadas')

    @staticmethod
    def get_rodadas_por_dt(dt:datetime.date) -> List[dict]:
        dt = datetime.date.today() if dt == None else dt

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
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['id','id_chuva','id_smap','id_previvaz','id_prospec','dt_rodada','hr_rodada','str_modelo','fl_preliminar','fl_pdp','fl_psat','fl_estudo','dt_revisao'])
        df['dt_rodada'] = df['dt_rodada'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')
    
    @staticmethod
    def get_historico_rodadas_por_nome(nome_rodada:str) -> List[dict]:
        query = db.select(
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada']
            ).where(
                CadastroRodadas.tb.c['str_modelo'] == nome_rodada
            ).distinct()
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['modelo', 'dt_rodada', 'hr_rodada'])
        return df.to_dict('records')
        
    
    @staticmethod
    def get_rodadas_por_dt_hr_nome(dt:datetime.datetime, nome:str) -> List[dict]:
        dt = datetime.datetime(datetime.date.today().year, datetime.date.today().month, datetime.date.today().day, 0) if dt == None else dt

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
        ).where(db.and_(
            CadastroRodadas.tb.c['dt_rodada'] == dt.date(),
            CadastroRodadas.tb.c['hr_rodada'] == dt.hour,
            CadastroRodadas.tb.c['str_modelo'] == nome            
        
        )).order_by(CadastroRodadas.tb.c['fl_preliminar'], CadastroRodadas.tb.c['fl_pdp'].desc(),CadastroRodadas.tb.c['fl_psat'].desc())
        
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['id','id_chuva','id_smap','id_previvaz','id_prospec','dt_rodada','hr_rodada','str_modelo','fl_preliminar','fl_pdp','fl_psat','fl_estudo','dt_revisao'])
        if df.empty:
            raise HTTPException(404, {"erro":f"Nenhum modelo encontrado com nome {nome} e data de rodada {dt}"})

        df['dt_rodada'] = df['dt_rodada'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')

    @staticmethod
    def info_rodadas(modelos:list, columns_data:list=[]): 
        if not columns_data: selected_columns = [column_name for column_name in CadastroRodadas.tb.columns] 
        else:
            conditional_columns = [
                CadastroRodadas.tb.c['id_chuva'] if 'id_chuva' in columns_data else None,
                CadastroRodadas.tb.c['id_smap'] if 'id_smap' in columns_data else None,
            ]
            base_columns = [
                CadastroRodadas.tb.c['id'],
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']
            ]

            selected_columns = base_columns + [col for col in conditional_columns if col is not None]

        order_priority = db.case(
                
                ((CadastroRodadas.tb.c['fl_pdp'] == 1) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 1),
                ((CadastroRodadas.tb.c['fl_psat'] == 1) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 2),
                ((CadastroRodadas.tb.c['fl_pdp'] == 0) & (CadastroRodadas.tb.c['fl_psat'] == 0) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 3),
                (CadastroRodadas.tb.c['fl_preliminar'] == 1, 4),
            
                else_=None
            ).label('order_priority')
        
        subquery_cadastro_rodadas = db.select(selected_columns + order_priority).where(
            db.tuple_(
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']
            ).in_(modelos)\
            )\
            .order_by(db.desc(CadastroRodadas.tb.c['dt_rodada']),db.asc(order_priority))

        rodadas_values = __DB__.db_execute(subquery_cadastro_rodadas, commit=prod).fetchall()
        return pd.DataFrame(rodadas_values, columns=[column_name.name for column_name in selected_columns]+['priority'])
    
    @staticmethod
    def get_last_column_id(column_name:str):
        query_get_max_id_column = db.select(db.func.max(CadastroRodadas.tb.c[column_name]))
        max_id = __DB__.db_execute(query_get_max_id_column, commit=prod).scalar()
        return max_id
    
    @staticmethod
    def inserir_cadastro_rodadas(rodadas_values:list):
        query_update = CadastroRodadas.tb.insert().values(rodadas_values)
        
        n_value = __DB__.db_execute(query_update, commit=prod).rowcount

        print(f"{n_value} Linhas inseridas na tb_cadastro_rodadas")

    @staticmethod
    def delete_por_id(id:int):
        query = CadastroRodadas.tb.delete(
        ).where(
         CadastroRodadas.tb.c['id'] == id           
                )
        n_value = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{n_value} Linhas deletadas na cadastro rodadas")
        return None
        
    @staticmethod
    def delete_rodada_chuva_smap_por_id_rodada(id_rodada:int):
        q_select = db.select(
            CadastroRodadas.tb.c['id'],
            CadastroRodadas.tb.c['id_chuva'],
            CadastroRodadas.tb.c['id_smap']
        ).where(
            CadastroRodadas.tb.c['id'] == id_rodada
        )
        
        result = __DB__.db_execute(q_select)
        try:
            ids = pd.DataFrame(result, columns=['id_rodada', 'id_chuva', 'id_smap']).to_dict('records')[0]
            Chuva.delete_por_id(ids['id_chuva'])
            Smap.delete_por_id(ids['id_smap'])
            CadastroRodadas.delete_por_id(ids['id_rodada'])
        except IndexError:
            print(f'id rodada {id_rodada} nao existe.')
       
       
        
class Chuva:
    tb:db.Table = __DB__.getSchema('tb_chuva')

    @staticmethod
    def get_chuva_por_id_subbacia(id_chuva:int):
        query = db.select(
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            Chuva.tb.c['cd_subbacia'],
            Chuva.tb.c['dt_prevista'],
            Chuva.tb.c['vl_chuva']
            ).where(db.and_(
                Chuva.tb.c['id'] == id_chuva
                )
            ).join(
                CadastroRodadas.tb, CadastroRodadas.tb.c['id_chuva'] == Chuva.tb.c['id']
        )
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['modelo', 'dt_rodada', 'hr_rodada', 'id', 'dt_prevista', 'vl_chuva'])
        df['dia_semana'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%A')
        df['dt_prevista'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()
        df = df.sort_values('dt_prevista')
        
        dfs = [g for _,g in df.groupby((pd.to_datetime(df['dt_prevista']) + pd.to_timedelta(1, unit='D')).dt.to_period('W'))]
        for i, item in enumerate(dfs):
            item['semana'] = i+1
            dfs[i] = item
            pass
        df = pd.concat(dfs)
        return df.to_dict('records')
            
    @staticmethod
    def get_chuva_por_id_data_entre_granularidade(
        id_chuva:int,
        granularidade:str,
        dt_inicio_previsao:Optional[datetime.date] = None,
        dt_fim_previsao:Optional[datetime.date] = None,
        no_cache: Optional[bool] = False,
        atualizar:Optional[bool] = False):
        
        if no_cache:
            df = pd.DataFrame(Chuva.get_chuva_por_id_subbacia(id_chuva))
        else:
            df = pd.DataFrame(cache.get_cached(Chuva.get_chuva_por_id_subbacia, id_chuva, atualizar=atualizar))
        if df.empty:
            return df
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime('%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id'])
        if granularidade == 'subbacia':
            df.rename(columns={'id':'cd_subbacia'}, inplace=True)
            return df.to_dict('records')
        df_subbacia = pd.DataFrame(Subbacia.get_subbacia())
        merged = df.merge(df_subbacia[['id', 'nome_bacia', 'nome_submercado']], on='id')
        if granularidade == 'bacia':
            df_bacia = pd.DataFrame(ons_service.tb_bacias.get_bacias('tb_chuva'))
            merged['nome_bacia'] = merged['nome_bacia'].str.upper()
            merged = merged.replace(
                'STA. MARIA DA VITÓRIA', 'SANTA MARIA VITORIA').replace(
                'SÃO FRANCI', 'SÃO FRANCISCO').replace(
                'JEQUITINHO', 'JEQUITINHONHA').replace(
                'PARANAPANE', 'PARANAPANEMA'
                )
            grouped = merged.groupby(['nome_bacia', 'dt_prevista', 'dia_semana', 'semana', 'hr_rodada', 'dt_rodada', 'modelo']).agg({'vl_chuva':'mean'}).reset_index()
            grouped = grouped.rename(columns={'nome_bacia':'nome'}).merge(df_bacia[['id', 'nome']], on='nome')
            grouped = grouped.drop(columns=['nome']).rename(columns={'id':'id_bacia'})
            return grouped.to_dict('records')

        if granularidade == 'submercado':
            df_submercado = pd.DataFrame(ons_service.tb_submercado.get_submercados())
            df_submercado['nome'] = df_submercado['nome'].str.capitalize()
            grouped = merged.groupby(['nome_submercado', 'dt_prevista', 'dia_semana', 'semana', 'hr_rodada', 'dt_rodada', 'modelo']).agg({'vl_chuva':'mean'}).reset_index()
            grouped = grouped.rename(columns={'nome_submercado':'nome'}).merge(df_submercado[['id', 'nome']], on='nome')
            grouped = grouped.drop(columns=['nome']).rename(columns={'id':'id_submercado'})
            return grouped.to_dict('records')
        
    @staticmethod
    def get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo, dt_hr_rodada, granularidade, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar):
        rodadas = CadastroRodadas.get_rodadas_por_dt_hr_nome(dt_hr_rodada, nome_modelo)
        return Chuva.get_chuva_por_id_data_entre_granularidade(rodadas[0]["id_chuva"], granularidade, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar)
        
    
    @staticmethod
    def get_previsao_chuva_modelos_combinados(
        query_obj: List[PesquisaPrevisaoChuva],
        granularidade:str,
        no_cache: Optional[bool] = False,
        atualizar:Optional[bool] = False):
        
        df = pd.DataFrame()
        for q in query_obj:
            df = pd.concat([df, pd.DataFrame(Chuva.get_chuva_por_id_data_entre_granularidade(q.id, q.dt_inicio, q.dt_fim, granularidade, no_cache, atualizar))])
        return df.to_dict('records')
    
    @staticmethod
    def export_rain(id_type: int) -> dict:
        
        def get_data_grouped(id_type: int, granularidade: str):
            return pd.DataFrame(Chuva.get_chuva_por_id_data_entre_granularidade(id_type, granularidade))
        
        df_map_grouped_by_subbacia = get_data_grouped(id_type, 'subbacia')
        df_map_grouped_by_bacia = get_data_grouped(id_type, 'bacia')
        df_map_grouped_by_submercado = get_data_grouped(id_type, 'submercado')
        
        df_subbacias = pd.DataFrame(Subbacia.get_subbacia())
        df_bacias = pd.DataFrame(ons_service.tb_bacias.get_bacias('tb_chuva'))
        df_submercados = pd.DataFrame(ons_service.tb_submercado.get_submercados())
        
        df_subbacias_merged = pd.merge(df_map_grouped_by_subbacia, df_subbacias, left_on='cd_subbacia', right_on='id')
        df_bacias_merged = pd.merge(df_map_grouped_by_bacia, df_bacias, left_on='id_bacia', right_on='id')
        df_submercados_merged = pd.merge(df_map_grouped_by_submercado, df_submercados, left_on='id_submercado', right_on='id')
        
        df_map_grouped_by_subbacia = df_subbacias_merged[['modelo','dt_rodada', 'hr_rodada', 'dt_prevista', 'nome', 'vl_chuva']]
        df_map_grouped_by_bacia = df_bacias_merged[['modelo','dt_rodada', 'dt_prevista', 'nome', 'vl_chuva']]
        df_map_grouped_by_submercado = df_submercados_merged[['modelo','dt_rodada', 'dt_prevista', 'str_sigla', 'vl_chuva']]
        
        df_model_base = df_map_grouped_by_subbacia[['modelo','dt_rodada', 'hr_rodada']].drop_duplicates()
        model_base = df_model_base.to_dict('records')[0]
    
        data_rodada_date = datetime.datetime.combine(model_base['dt_rodada'], datetime.time(0))
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
                valorAgrupamento = value['nome'] if tipo in ['subbacia', 'bacia'] else value['str_sigla']
                data_referente_date = datetime.datetime.strptime(value['dt_prevista'], '%Y-%m-%d')
            
                agrupamentos[tipo]['valoresMapa'].append({
                    "valor": value['vl_chuva'],
                    "dataReferente": value['dt_prevista'],
                    "valorAgrupamento": valorAgrupamento
                })
            
                if data_final is None or data_referente_date > data_final:
                    data_final = data_referente_date
                    data_final_str = value['dt_prevista']

        data = [agrup for agrup in agrupamentos.values() if agrup['valoresMapa']]

        body = {
            "dataRodada": data_rodada_str,
            "dataFinal": data_final_str,
            "mapType": "chuva",
            "idType": str(id_type),
            "modelo": model_base['modelo'],
            "priority": None,
            "grupo": grupo,
            "rodada": str(model_base['hr_rodada']),
            "viez": viez,
            "membro": "0",
            "measuringUnit": "mm",
            "propagationBase": None,
            "generationProcess": "SMAP",
            "data": data
        }
        
        
        accessToken = get_access_token();
        api_url = F'{settings.API_URL}/map'
        
        
        res = r.post(api_url, verify=False, json=body, headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {accessToken}'})
        
        try:
            res.raise_for_status()  
        except HTTPError as http_err:
            logger.error(f"HTTP error: {http_err}")
        except Exception as err:
            logger.error(f"Other error: {err}")
        else:
            if res.status_code == 201:
                logger.info(f"Modelo {modelo} do dia {data_rodada_str} inserido no endereco ${api_url}")
            else:
                logger.warning(f"Erro ao tentar inserir o modelo {modelo} do dia {data_rodada_str} no endereco ${api_url}")
        
        
    
    @staticmethod
    def post_chuva_modelo_combinados(chuva_prev:List[ChuvaPrevisaoCriacao], rodar_smap:bool, prev_estendida:bool) -> None:
        
        prevs:List[dict] = []
        for prev in chuva_prev:
            prevs.append(prev.model_dump())

        try:
            id_rodada = CadastroRodadas.get_rodadas_por_dt_hr_nome(prevs[0]['dt_rodada'], prevs[0]['modelo'])[0]['id']
            CadastroRodadas.delete_rodada_chuva_smap_por_id_rodada(id_rodada)
        except HTTPException:
            print("Modelo nao encontrado.")
        
        modelo = (prevs[0]['modelo'], prevs[0]['dt_rodada'].hour , prevs[0]['dt_rodada'].date())
        
        for i in range(len(prevs)):
            prevs[i]['dt_rodada'] = prevs[i]['dt_rodada'].date()
            
        df = pd.DataFrame(prevs)
        df['cenario'] = f'{modelo[0]}_{modelo[1]}_{modelo[2]}'
        
        id_chuva = Chuva.inserir_chuva_modelos(df, rodar_smap, prev_estendida)
        
        Chuva.export_rain(id_chuva)
        
        return None
    
    @staticmethod
    def inserir_prev_chuva(df_prev_vazao_out:pd.DataFrame):
        values_chuva = df_prev_vazao_out[['id_chuva','cd_subbacia','dt_prevista','vl_chuva']].values.tolist()
        query_insert = Chuva.tb.insert().values(values_chuva)
        n_value = __DB__.db_execute(query_insert, commit=prod).rowcount
        print(f"{n_value} Linhas inseridas na Chuva") 
        
    @staticmethod
    def inserir_chuva_modelos(df_prev_chuva_out:pd.DataFrame, rodar_smap:bool, prev_estendida:bool): 
        df_info_subbacias = Subbacia.info_subbacias()
        df_chuva_final = pd.merge(df_info_subbacias[['cd_subbacia' ,'vl_lon'  ,'vl_lat']], df_prev_chuva_out)
        df_prev_chuva = df_chuva_final.drop(['vl_lat','vl_lon'],axis=1)
        # df_prev_chuva = df_chuva_final.melt(id_vars=['cd_subbacia','cenario', 'dt_prevista'] , value_vars='vl_chuva')

        """
        REMOVENDO CadastroRodadas.info_rodadas e retornando dataframe vazio diretamente
        """                
        # df_info_rodadas = CadastroRodadas.info_rodadas(modelos)

        df_info_rodadas = pd.DataFrame(columns=['str_modelo', 'dt_rodada', 'hr_rodada'])
        
        new_chuva_id = CadastroRodadas.get_last_column_id('id_chuva') + 1
        insert_cadastro_values = []
        for cenario in df_prev_chuva['cenario'].unique():

            str_modelo ,hr_rodada, dt_rodada = cenario.split('_')
            
            mask_id_chuva = \
            (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & \
            (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & \
            (df_info_rodadas['hr_rodada'] == int(hr_rodada))

            if df_info_rodadas[mask_id_chuva].empty:
                insert_cadastro_values += [None, new_chuva_id, None, None,None,dt_rodada,int(hr_rodada),str_modelo,None,None,None,None,None],
                df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = new_chuva_id

            else:
                df_info_rodadas['id_chuva'] = df_info_rodadas['id_chuva'].astype(str).str.replace('nan','None')
                id_chuva = df_info_rodadas[mask_id_chuva]['id_chuva'].unique()[0]
                print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada com id_chuva: {id_chuva}")
                
                if id_chuva != 'None':
                    df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = id_chuva
                else:
                    print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada porem sem id_chuva, será cadastrado com id_chuva: {new_chuva_id}")
                    df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = new_chuva_id


            if insert_cadastro_values: CadastroRodadas.inserir_cadastro_rodadas(insert_cadastro_values)
            Chuva.inserir_prev_chuva(df_prev_chuva.round(2))
            
            if rodar_smap:
                Smap.post_rodada_smap(RodadaSmap.model_validate({'dt_rodada':datetime.datetime.strptime(dt_rodada, '%Y-%m-%d'),'hr_rodada':hr_rodada,'str_modelo':str_modelo}), prev_estendida)
            return new_chuva_id

    @staticmethod
    def delete_por_id(id:int):
        query = Chuva.tb.delete(
        ).where(
         Chuva.tb.c['id'] == id           
                )
        n_value = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{n_value} Linhas deletadas na Chuva")
        return None
    
class ChuvaMembro:
    tb:db.Table = __DB__.getSchema('tb_chuva_membro')
    @staticmethod
    def post_chuva_membro(chuva_prev:List[ChuvaPrevisaoCriacaoMembro], rodar_smap:bool) -> None:
        records = [obj.model_dump() for obj in chuva_prev]
        df = pd.DataFrame(records)
        # dt_hr_rodada:datetime.datetime = df[["dt_hr_rodada"]].drop_duplicates().to_dict("records")[0]["dt_hr_rodada"].to_pydatetime()
        # modelo:str = df[["modelo"]].drop_duplicates().to_dict("records")[0]["modelo"]
        # df_membro_modelo["id_rodada"] = CadastroRodadas.get_rodadas_por_dt_hr_nome(dt_hr_rodada, modelo)[0]["id"]
        df['dt_hr_rodada'] = df['dt_hr_rodada'].apply(lambda x: x.replace(minute=0, second=0, microsecond=0))
        df = df.rename(columns={"membro":"nome"})
        df_membro_modelo = df[["nome", "dt_hr_rodada", "modelo", "peso"]].drop_duplicates(['nome', 'dt_hr_rodada'])

        df_membro_modelo = pd.DataFrame(MembrosModelo.inserir(df_membro_modelo.to_dict("records")))
        df = df.merge(df_membro_modelo, on="nome").rename(columns={"id":"id_membro_modelo"})[["id_membro_modelo", "vl_chuva", "cd_subbacia", "dt_prevista"]]
        
        body = df.to_dict("records")
        ChuvaMembro.inserir(body)
        ChuvaMembro.media_membros(chuva_prev[0].dt_hr_rodada.replace(minute=0, second=0, microsecond=0), chuva_prev[0].modelo, inserir=True, rodar_smap=rodar_smap)
        return None
    
    @staticmethod
    def inserir(body:List[dict]):
        id_membro_modelo = []
        cd_subbacia = []
        dt_prevista = []
        for membro in body:
            id_membro_modelo.append(membro["id_membro_modelo"])
            cd_subbacia.append(membro["cd_subbacia"])
            dt_prevista.append(membro["dt_prevista"])
            
        search_params = (ChuvaMembro.tb.c["id_membro_modelo"].in_(id_membro_modelo),
                ChuvaMembro.tb.c["cd_subbacia"].in_(cd_subbacia),
                ChuvaMembro.tb.c["dt_prevista"].in_(dt_prevista))
        
        q_delete = ChuvaMembro.tb.delete().where(db.and_(
            *search_params
        ))
        linhas_delete = __DB__.db_execute(q_delete, commit=prod).rowcount
        print(f"{linhas_delete} linhas inseridas chuva membro")

        query = ChuvaMembro.tb.insert().values(body)
        linhas_insert = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{linhas_insert} linhas inseridas chuva membro")

    @staticmethod
    def media_membros(dt_hr_rodada:datetime.datetime, modelo:str, inserir:bool = False, prev_estendida:bool = False, rodar_smap:bool = False) -> None:
        q_select = db.select(
            ChuvaMembro.tb.c['cd_subbacia'],
            ChuvaMembro.tb.c['dt_prevista'],
            ChuvaMembro.tb.c['vl_chuva']
        ).join(
            MembrosModelo.tb, MembrosModelo.tb.c['id'] == ChuvaMembro.tb.c['id_membro_modelo']
        ).where(
            db.and_(
                MembrosModelo.tb.c['dt_hr_rodada'] == dt_hr_rodada,
                MembrosModelo.tb.c['modelo'] == modelo
            )
        )
        result = __DB__.db_execute(q_select, commit=prod)
        df = pd.DataFrame(result, columns=['cd_subbacia', 'dt_prevista', 'vl_chuva'])
        df = df.groupby(['cd_subbacia', 'dt_prevista']).mean().reset_index()
        df['modelo'] = modelo
        df['dt_rodada'] = dt_hr_rodada
        df['dt_rodada'] = pd.Series(df['dt_rodada'].dt.to_pydatetime(), dtype = object)
        if inserir:
            Chuva.post_chuva_modelo_combinados([ChuvaPrevisaoCriacao.model_validate(x) for x in df.to_dict('records')], rodar_smap, prev_estendida)
    @staticmethod
    def get_chuva_por_nome_modelo_dt_hr(nome_modelo, dt_hr_rodada):
        query = db.select(
            MembrosModelo.tb.c['nome'],
            MembrosModelo.tb.c['modelo'],
            MembrosModelo.tb.c['dt_hr_rodada'],
            ChuvaMembro.tb.c['cd_subbacia'],
            ChuvaMembro.tb.c['dt_prevista'],
            ChuvaMembro.tb.c['vl_chuva']
            ).where(db.and_(
                MembrosModelo.tb.c['modelo'] == nome_modelo,
                MembrosModelo.tb.c['dt_hr_rodada'] == dt_hr_rodada
                )
            ).join(
                MembrosModelo.tb, MembrosModelo.tb.c['id'] == ChuvaMembro.tb.c['id_membro_modelo']
            
        )
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['membro', 'modelo', 'dt_hr_rodada', 'id', 'dt_prevista', 'vl_chuva'])
        df['dia_semana'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%A')
        df['dt_prevista'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()
        df = df.sort_values('dt_prevista')
        
        dfs = [g for _,g in df.groupby((pd.to_datetime(df['dt_prevista']) + pd.to_timedelta(1, unit='D')).dt.to_period('W'))]
        for i, item in enumerate(dfs):
            item['semana'] = i+1
            dfs[i] = item
            pass
        df = pd.concat(dfs)
        return df.to_dict('records')

    @staticmethod
    def get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo, dt_hr_rodada, granularidade, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar):
        
        if no_cache:
            df = pd.DataFrame(ChuvaMembro.get_chuva_por_nome_modelo_dt_hr(nome_modelo, dt_hr_rodada))
        else:
            df = pd.DataFrame(cache.get_cached(ChuvaMembro.get_chuva_por_nome_modelo_dt_hr, nome_modelo, dt_hr_rodada, atualizar=atualizar))
        if df.empty:
            return list()
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime('%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id', 'membro'])
        if granularidade == 'subbacia':
            df.rename(columns={'id':'cd_subbacia'}, inplace=True)
            return df.to_dict('records')
        
class Subbacia:
    tb:db.Table = __DB__.getSchema('tb_subbacia')
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
        result = __DB__.db_execute(query, commit=prod)
        df = pd.DataFrame(result, columns=['id', 'nome', 'nome_submercado', 'nome_bacia', 'vl_lat', 'vl_lon', 'nome_smap', 'pasta_contorno', 'cd_bacia_mlt'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
    @staticmethod
    def get_bacias():
        query = db.select(
            db.distinct(Subbacia.tb.c['txt_bacia'])
        )
        result = __DB__.db_execute(query, commit=prod)
        df = pd.DataFrame(result, '')
        
    @staticmethod
    def info_subbacias():
        query = db.select(Subbacia.tb.c['cd_subbacia'],Subbacia.tb.c['vl_lon'],Subbacia.tb.c['vl_lat'],Subbacia.tb.c['txt_nome_subbacia'])
        answer_tb_subbacia = __DB__.db_execute(query, commit=prod)
        
        df_subbac = pd.DataFrame(answer_tb_subbacia, columns=['cd_subbacia','vl_lon','vl_lat','nome'])
        df_subbac['nome'] = df_subbac['nome'].str.lower()
        return df_subbac
         
class Smap:
    tb:db.Table = __DB__.getSchema('tb_smap')
    @staticmethod
    def post_rodada_smap(rodada:RodadaSmap, prev_estendida:bool):
        momento_req:datetime.datetime = datetime.datetime.now()
        # trigger_dag_SMAP(rodada.dt_rodada, [rodada.str_modelo], rodada.hr_rodada, momento_req)
        airflow_service.trigger_airflow_dag(
            dag_id="PREV_SMAP",
            json_produtos={
                "modelos":[[
                    rodada.str_modelo,
                    rodada.hr_rodada,
                    rodada.dt_rodada.strftime("%Y-%m-%d")
                    ]
                ],
                "prev_estendida":prev_estendida
            },
            momento_req=momento_req

        )
        
        return None
        # response:dict = get_dag_smap_run_status(rodada.str_modelo, momento_req)
        
        # falhou = response['state'] == 'failed'
        
        # Smap.enviar_email_status_smap(falhou, response['end_datetime'], response["url"])
        # if falhou:
        #     raise HTTPException(400, f'{response}')
        # return response
    
    # @staticmethod
    # def enviar_email_status_smap(sucesso: bool, momento:datetime.datetime, dag_url:str):
    #     email = WxEmail()
        
    #     status, cor_status = ("Concluido", "#88B04B") if sucesso else ("Falha", "#b04b4b")
        
    #     style = f"""<style> body {{ font-family: Arial, Helvetica, sans-serif; text-align: center; padding: 40px 0; margin: 0; background: #EBF0F5; }} h1 {{ color: {cor_status}; font-weight: 900; font-size: 40px; margin-bottom: 10px; }} p {{ color: #404F5E; font-size: 20px; margin: 0; }} .card {{ background: white; padding: 30px; border-radius: 4px; box-shadow: 0 2px 3px #C8D0D8; display: inline-block; margin: 0 auto; }}</style>"""
    #     html = f"""<html><head></head>{style}<body> <div class="card"> <h1>{status}</h1> <p>Fim da execução do Airflow {momento.strftime("%d/%m/%Y %H:%M:%S")}<br /><a href="{dag_url}">DAG Airflow</a></p> </div></body></html>"""
        
    #     email.sendEmail(texto=html, assunto="SMAP - Gera Chuva", send_to=["arthur.moraes@raizen.com"])

    @staticmethod
    def delete_por_id(id:int):
        query = Smap.tb.delete(
        ).where(
         Smap.tb.c['id'] == id           
                )
        n_value = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{n_value} Linhas deletadas tb smap")
        return None

class MembrosModelo:
    tb:db.Table = __DB__.getSchema('tb_membro_modelo')
    
    @staticmethod
    def inserir(body:List[dict]) -> List[dict]:
        dt_hr_rodada = []
        nome = []
        modelo = []
        for membro in body:
            dt_hr_rodada.append(membro["dt_hr_rodada"])
            nome.append(membro["nome"])
            modelo.append(membro["modelo"])
            
        search_params = (MembrosModelo.tb.c["dt_hr_rodada"].in_(dt_hr_rodada),
                MembrosModelo.tb.c["nome"].in_(nome),
                MembrosModelo.tb.c["modelo"].in_(modelo))
        q_delete = MembrosModelo.tb.delete().where(db.and_(
            *search_params
        ))
        linhas_delete = __DB__.db_execute(q_delete, commit=prod).rowcount
        print(f'{linhas_delete} linha(s) deletada(s) tb membro modelo')
        q_insert = MembrosModelo.tb.insert().values(body)
        linhas_insert =  __DB__.db_execute(q_insert, commit=prod).rowcount
        print(f'{linhas_insert} linha(s) inserida(s) tb membro modelo')
        q_select = db.select(
            MembrosModelo.tb.c["id"],
            MembrosModelo.tb.c["dt_hr_rodada"],
            MembrosModelo.tb.c["nome"],
            MembrosModelo.tb.c["modelo"]
            ).where(db.and_(
                *search_params
            )
                    )
        result = __DB__.db_execute(q_select, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=["id", "dt_hr_rodada", "nome", "modelo"])
        return df.to_dict("records")
        
        
    @staticmethod
    def get_chuva_por_id_data_entre_granularidade(
        id_chuva:int,
        granularidade:str,
        dt_inicio_previsao:Optional[datetime.date] = None,
        dt_fim_previsao:Optional[datetime.date] = None,
        no_cache: Optional[bool] = False,
        atualizar:Optional[bool] = False):
        
        if no_cache:
            df = pd.DataFrame(Chuva.get_chuva_por_id_subbacia(id_chuva))
        else:
            df = pd.DataFrame(cache.get_cached(Chuva.get_chuva_por_id_subbacia, id_chuva, atualizar=atualizar))
        if df.empty:
            return df
        if dt_inicio_previsao is not None and dt_fim_previsao is not None:
            df = df[(df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim_previsao.strftime('%Y-%m-%d'))]
        elif dt_inicio_previsao is not None:
            df = df[df['dt_prevista'] >= dt_inicio_previsao.strftime('%Y-%m-%d')]
        df = df.sort_values(['dt_prevista', 'id'])
        if granularidade == 'subbacia':
            df.rename(columns={'id':'cd_subbacia'}, inplace=True)
            return df.to_dict('records')
    
class ChuvaObs:
    tb:db.Table = __DB__.getSchema('tb_chuva_obs')
    
    @staticmethod
    def post_chuva_obs(
        chuva_obs:List[ChuvaObsReq]
    ):
        chuva_obs = [c.model_dump() for c in chuva_obs]
        df = pd.DataFrame(chuva_obs)
        df['dt_observado'].to_list()
        
        query_delete = ChuvaObs.tb.delete(
            ).where(db.and_(
                   ChuvaObs.tb.c['dt_observado'] == chuva_obs[0]['dt_observado']
            ))
        rows_delete = __DB__.db_execute(query_delete, prod).rowcount
        print(f'{rows_delete} linha(s) deletada(s)')
        query_insert = ChuvaObs.tb.insert(
            ).values(
            df.to_dict('records')
        )
        rows_insert = __DB__.db_execute(query_insert, prod).rowcount
        print(f'{rows_insert} linha(s) inserida(s)')
        
    @staticmethod
    def get_chuva_observada_por_data(
        dt_observado:datetime.date
    ):
        query_select = db.select(
            ChuvaObs.tb.c['cd_subbacia'],
            ChuvaObs.tb.c['dt_observado'],
            ChuvaObs.tb.c['vl_chuva']
        ).where(
            ChuvaObs.tb.c['dt_observado'] == dt_observado
        )
        result = __DB__.db_execute(query_select, prod)
        df = pd.DataFrame(result, columns=['cd_subbacia', 'dt_observado', 'vl_chuva'])
        return df.to_dict('records')


class ChuvaObsPsat:
    tb:db.Table = __DB__.getSchema('tb_chuva_psat')
    
    @staticmethod
    def post_chuva_obs_psat(
        chuva_obs:List[ChuvaObsReq]
    ):
        chuva_obs = [c.model_dump() for c in chuva_obs]
        df = pd.DataFrame(chuva_obs)
        df['dt_observado'].to_list()
        
        query_delete = ChuvaObsPsat.tb.delete(
            ).where(db.and_(
                   ChuvaObsPsat.tb.c['dt_ini_observado'] == chuva_obs[0]['dt_observado']
            ))
        rows_delete = __DB__.db_execute(query_delete, prod).rowcount
        print(f'{rows_delete} linha(s) deletada(s)')
        query_insert = ChuvaObsPsat.tb.insert(
            ).values(
            df.to_dict('records')
        )
        rows_insert = __DB__.db_execute(query_insert, prod).rowcount
        print(f'{rows_insert} linha(s) inserida(s)')
        
    @staticmethod
    def get_chuva_observada_psat_por_data(
        dt_observado:datetime.date
    ):
        query_select = db.select(
            ChuvaObsPsat.tb.c['cd_subbacia'],
            ChuvaObsPsat.tb.c['dt_ini_observado'],
            ChuvaObsPsat.tb.c['vl_chuva']
        ).where(
            ChuvaObsPsat.tb.c['dt_ini_observado'] == dt_observado
        )
        result = __DB__.db_execute(query_select, prod)
        df = pd.DataFrame(result, columns=['cd_subbacia', 'dt_observado', 'vl_chuva'])
        return df.to_dict('records')
        

    
if __name__ == '__main__':
    ChuvaMembro.media_membros(datetime.datetime(2024,10,24,12,0,0), 'string')
    # Smap.enviar_email_status_smap(False, datetime.datetime.now(), "https://tradingenergiarz.com/airflow/dags/ONS_DADOS_ABERTOS/grid?tab=graph&dag_run_id=scheduled__2024-09-17T20%3A00%3A00%2B00%3A00")
    # CadastroRodadas.get_rodadas_por_dt(datetime.date.today())
    # teste = MembrosModeloSchema()
    
    # MembrosModelo.inserir()
    # Chuva.get_chuva_por_id_data_entre_granularidade(11438, datetime.date.today(),datetime.date.today()+datetime.timedelta(days=15), 'bacia')
    pass