import pdb
import datetime
import numpy as np
import numpy as np
import pandas as pd
from sys import path
import sqlalchemy as sa
import sqlalchemy as db
from typing import List, Optional
from fastapi import HTTPException
from app.core.utils.logger import logging
from app.core.utils.date_util import MONTH_DICT, ElecData
from app.core.database.wx_dbClass import db_mysql_master
from .schema import *

logger = logging.getLogger(__name__)


prod = True
__DB__ = db_mysql_master('db_decks')

class WeolSemanal:
    tb:db.Table = __DB__.getSchema('tb_dc_weol_semanal')
    @staticmethod
    def create(body: List[WeolSemanalSchema]):
        body_dict = [x.model_dump() for x in body]
        delete_dates = list(set([x['data_produto'] for x in body_dict]))
        for date in delete_dates:
            WeolSemanal.delete_by_product_date(date)
        query = db.insert(WeolSemanal.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def delete_by_product_date(date:datetime.date):
        query = db.delete(WeolSemanal.tb).where(WeolSemanal.tb.c.data_produto == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def get_all():
        query = db.select(WeolSemanal.tb)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        result = result.to_dict('records')
        return result
    
    @staticmethod
    def get_by_product_date_start_week_year_month_rv(data_produto:datetime.date, mes_eletrico:int, ano:int, rv:int):
        query = db.select(
            WeolSemanal.tb
            ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.mes_eletrico == mes_eletrico,
                db.func.year(WeolSemanal.tb.c.inicio_semana) == ano,
                WeolSemanal.tb.c.rv_atual == rv
            ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')

    @staticmethod
    def get_by_product_start_week_date_product_date(inicio_semana:datetime.date, data_produto:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.inicio_semana >= inicio_semana
            ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')
    
    @staticmethod
    def get_by_product_date(data_produto:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(WeolSemanal.tb.c.data_produto == data_produto)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana', 'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(status_code=404, detail=f"Produto da data {data_produto} não encontrado")
        return result.to_dict('records')
    
    @staticmethod
    def get_by_product_date_between(start_date:datetime.date, end_date:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(WeolSemanal.tb.c.data_produto.between(start_date, end_date))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana', 'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(status_code=404, detail=f"Produtos entre as datas {start_date} e {end_date} não encontrados")
        return result.to_dict('records')

    @staticmethod
    def get_weighted_avg_by_product_date(product_date:datetime.date, ):
        df = pd.DataFrame(WeolSemanal.get_by_product_date(product_date))
        df_horas_por_patamar = pd.DataFrame(Patamares.get_horas_por_patamar_por_inicio_semana_data(df['inicioSemana'].min(), df['finalSemana'].max()))
        merged_df = pd.merge(df, df_horas_por_patamar, on=['inicioSemana', 'patamar'], how='left')
        
        df_weighted = merged_df[['dataProduto', 'inicioSemana', 'qtdHoras']][merged_df['submercado'] == "S"]
        df_weighted = df_weighted.groupby(['dataProduto', 'inicioSemana']).agg({'qtdHoras':'sum'}).rename({'qtdHoras':'totalHoras'}, axis=1)
        
        merged_df = pd.merge(df_weighted, merged_df, on=['dataProduto', 'inicioSemana'], how='left')

        df_group  = merged_df[['dataProduto','inicioSemana','patamar','valor','qtdHoras','totalHoras', 'submercado']]
        
        df_group['mediaPonderada'] = df_group['valor'] * df_group['qtdHoras']
        
        df_group = df_group.groupby(['dataProduto', 'inicioSemana', 'submercado']).agg({'mediaPonderada':'sum', 'totalHoras':'max'}).reset_index()
        
        df_group['mediaPonderada'] = df_group['mediaPonderada'] / df_group['totalHoras']
        df_group.sort_values(by=['submercado', 'inicioSemana'], inplace=True)
        

        
        return df_group.to_dict('records')

    @staticmethod
    def get_weighted_avg_by_product_date_between(start_date:datetime.date, end_date:datetime.date):
        df = pd.DataFrame(WeolSemanal.get_by_product_date_between(start_date, end_date))
        
        df_horas_por_patamar = pd.DataFrame(Patamares.get_horas_por_patamar_por_inicio_semana_data(df['inicioSemana'].min(), df['finalSemana'].max()))
        merged_df = pd.merge(df, df_horas_por_patamar, on=['inicioSemana', 'patamar'], how='left')
        
        df_weighted = merged_df[['dataProduto', 'inicioSemana', 'qtdHoras']][merged_df['submercado'] == "S"]
        df_weighted = df_weighted.groupby(['dataProduto', 'inicioSemana']).agg({'qtdHoras':'sum'}).rename({'qtdHoras':'totalHoras'}, axis=1)
        
        merged_df = pd.merge(df_weighted, merged_df, on=['dataProduto', 'inicioSemana'], how='left')
        df_group  = merged_df.groupby(['dataProduto', 'inicioSemana', 'patamar']).agg({'valor':'sum', 'qtdHoras':'max', 'totalHoras':'max'}).reset_index()
        
        df_group['mediaPonderada'] = df_group['valor'] * df_group['qtdHoras']
        df_group = df_group.groupby(['dataProduto', 'inicioSemana']).agg({'mediaPonderada':'sum', 'totalHoras':'max'}).reset_index()
        df_group['mediaPonderada'] = df_group['mediaPonderada'] / df_group['totalHoras']
        

        df = df_group.pivot(index=['inicioSemana'], columns='dataProduto', values='mediaPonderada').reset_index()
        return df.to_dict('records')

    @staticmethod
    def get_html_weighted_avg(df: pd.DataFrame):
        html:str = '''<style> body { font-family: sans-serif; } th, td { padding: 4px; text-align: center; border: 0.5px solid; } table { border-collapse: collapse; } thead, .gray { background-color: #d9d9d9; border: 1px solid; } .none{ background-color: #e6e6e6; } tbody *{ border: none; } tbody{ border: 1px solid; } .n1{background-color: #63be7b;} .n2{background-color: #aad380;} .n3{background-color: #efe784;} .n4{background-color: #fcbc7b;} .n5{background-color: #fba777;} .n6{background-color: #f8696b;}</style><table> <thead> <tr>'''
        for col in df.columns:
            html += f'<th>{col}</th>'
        html += ' </tr></thead><tbody>'
            
        eolica_newave = df[df['Origem'] == 'Eolica Newave'] 
           
        for i, row in df.iterrows():
            html += '<tr>'
            for j, col in enumerate(row):
                if j == 0:
                    html += f'<td class="gray"><strong>{col}</strong></td>'
                else:
                    if bool(np.isnan(col)):
                        html += f'<td class="none"></td>'
                        continue
                    elif row['Origem'] == 'Eolica Newave':
                        html += f'<td class="n3">{int(col)}</td>'
                        continue
                    percent_diff:float = col / eolica_newave.iloc[0].iloc[j]
                    if percent_diff >= 1.3:
                        html += f'<td class="n1">{int(col)}</td>'  # Acima de 30% da Eólica Newave
                    elif percent_diff >= 1.1:
                        html += f'<td class="n2">{int(col)}</td>'  # Entre 10% e 30%
                    elif percent_diff >= 0.9:
                        html += f'<td class="n3">{int(col)}</td>'  # Entre 10% acima e 10% abaixo
                    elif percent_diff >= 0.8:
                        html += f'<td class="n4">{int(col)}</td>'  # Entre 10% e 20% abaixo
                    elif percent_diff >= 0.6:
                        html += f'<td class="n5">{int(col)}</td>'  # Entre 20% e 40% abaixo
                    else:
                        html += f'<td class="n6">{int(col)}</td>'  # Menor que 40% da Eólica Newave
                        
            html += '</tr>'
        html += '</tbody></table>'
        # with open('/WX2TB/Documentos/fontes/index.html', 'w') as f:
        #     f.write(html)
        return {"html" : html}


    @staticmethod
    def get_weighted_avg_table_monthly_by_product_date(data_produto:datetime.date, quantidade_produtos:int):
        df = pd.DataFrame(WeolSemanal.get_weighted_avg_by_product_date_between(data_produto - datetime.timedelta(days=quantidade_produtos-1), data_produto))
        
        df_eol_newave = pd.DataFrame(NwSistEnergia.get_eol_by_last_data_deck_mes_ano_between(df['inicioSemana'][0], df['inicioSemana'][len(df['inicioSemana'])-1]))

        df_eol_newave = df_eol_newave.groupby(['mes', 'ano']).agg({'geracaoEolica':'sum'}).reset_index()
        df_eol_newave['yearMonth'] = df_eol_newave['ano'].astype(str) + '-' + df_eol_newave['mes'].astype(str)
        columns_rename = [MONTH_DICT[int(row['mes'])] + f' {int(row["ano"])}' for i, row in df_eol_newave.iterrows()]

        df['ano'] = [row.year if type(row) != str else row for row in df['inicioSemana']]
        df['mes'] = [row.month if type(row) != str else row for row in df['inicioSemana']]

        df_eol_newave = df_eol_newave.sort_values(by='yearMonth')
        df_eol_newave.drop(columns=['mes', 'ano'], inplace=True)
        
        df['yearMonth'] = df['inicioSemana'].apply(lambda x: f'{x.year}-{x.month}' if type(x) != str else x)

        df.drop(columns=['inicioSemana'], inplace=True)
        df = df.groupby('yearMonth').mean()


        df = pd.merge(df_eol_newave,df, on='yearMonth', how='left')
        
        df.drop(columns=['mes', 'ano'], inplace=True)
        
        df['yearMonth'] = columns_rename
        df.rename(columns={'yearMonth': 'Origem', 'geracaoEolica':'Eolica Newave'}, inplace=True)
        
        df.columns = [df.columns[0]] + [(x + datetime.timedelta(days=1)).strftime('WEOL %d/%m') if type(x) != str else x for x in df.columns[1:]]
        df = df[[df.columns[1], df.columns[0]] +  df.columns[2:].to_list()]

        df = df.transpose()
        df.reset_index(inplace=True)
        df.columns = df.iloc[0]
        df = df[1:]
           
        return WeolSemanal.get_html_weighted_avg(df)

    @staticmethod
    def get_weighted_avg_table_weekly_by_product_date(data_produto:datetime.date, quantidade_produtos:int):
        df = pd.DataFrame(WeolSemanal.get_weighted_avg_by_product_date_between(data_produto - datetime.timedelta(days=quantidade_produtos), data_produto))

        df_eol_newave = pd.DataFrame(NwSistEnergia.get_eol_by_last_data_deck_mes_ano_between(df['inicioSemana'][0], df['inicioSemana'][len(df['inicioSemana'])-1]))
        df_eol_newave = df_eol_newave.groupby(['mes', 'ano']).agg({'geracaoEolica':'sum'}).reset_index()
        df_eol_newave = df_eol_newave.sort_values(by=['ano', 'mes'])

        df['ano'] = [ElecData(row).inicioSemana.year if type(row) != str else row for row in df['inicioSemana']]
        df['mes'] = [ElecData(row).mesReferente if type(row) != str else row for row in df['inicioSemana']]

        df = pd.merge(df_eol_newave,df, on=['ano', 'mes'], how='left')
        df.drop(columns=['mes', 'ano'], inplace=True)
        
        df.rename(columns={'inicioSemana': 'Origem', 'geracaoEolica':'Eolica Newave'}, inplace=True)
        
        df.columns = [df.columns[0]] + [(x + datetime.timedelta(days=1)).strftime('WEOL %d/%m') if type(x) != str else x for x in df.columns[1:]]
        df = df[[df.columns[1], df.columns[0]] +  df.columns[2:].to_list()]

        df = df.transpose()
        df.reset_index(inplace=True)
        
        df.columns = df.iloc[0]
        df = df[1:]
        df.drop(columns=[np.nan], inplace=True, errors='ignore')
        df.columns = [df.columns[0]] + [f'{MONTH_DICT[ElecData(x).mesReferente]}-rv{ElecData(x).atualRevisao}' if type(x) != str else x for x in df.columns[1:]]
        return WeolSemanal.get_html_weighted_avg(df)

class Patamares:
    tb:db.Table = __DB__.getSchema('tb_patamar_decomp')
    @staticmethod
    def create(body: List[PatamaresDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        dates = list(set([x['inicio'] for x in body_dict]))
        dates.sort()
        Patamares.delete_by_start_date_between(dates[0].date(), dates[-1].date())
        query = db.insert(Patamares.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date(date:datetime.date):
        query = db.delete(Patamares.tb).where(db.func.date(Patamares.tb.c.inicio) == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None
    
    @staticmethod
    def delete_by_start_date_between(start:datetime.date, end:datetime.date):
        query = db.delete(Patamares.tb).where(db.func.date(Patamares.tb.c.inicio).between(start, end))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None
    
    @staticmethod
    def get_horas_por_patamar_por_inicio_semana_data(inicio_semana:datetime.date, fim_semana:datetime.date):
        query = db.select(
            db.func.count(),
            Patamares.tb.c["patamar"],
            db.func.min(db.func.date(Patamares.tb.c["inicio"]))
        ).where(
            db.func.date(db.func.date_sub(Patamares.tb.c["inicio"], db.text("interval 1 hour"))).between(inicio_semana, fim_semana)
        ).group_by(
            Patamares.tb.c["semana"],
            Patamares.tb.c["patamar"]
        )
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=["qtdHoras", "patamar","inicio"])
        result = result.sort_values(by=["inicio", "patamar"])
        
        for i in range(2, len(result), 3):
            result.at[i, 'inicio'] = result.at[i-1, 'inicio']
        
        result.loc[(result['patamar'] != 'Pesada') & (result['patamar'] != 'Leve'), 'patamar'] = 'medio'
        result.loc[result['patamar'] == 'Pesada', 'patamar'] = 'pesado'
        result.loc[result['patamar'] == 'Leve', 'patamar'] = 'leve'
        result = result.rename(columns={'inicio': 'inicioSemana'})
        return result.to_dict("records")
    
class NwSistEnergia:
    tb:db.Table = __DB__.getSchema('tb_nw_sist_energia')
    
    @staticmethod
    def get_last_data_deck():
        query = db.select(
            db.func.max(NwSistEnergia.tb.c["dt_deck"])
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=['dt_deck'])                              
        return df.to_dict('records')
    
    @staticmethod
    def get_eol_by_last_data_deck_mes_ano_between(start:datetime.date, end:datetime.date):
        start = start.replace(day=1)
        end = end.replace(day=1)
        last_data_deck = NwSistEnergia.get_last_data_deck()[0]["dt_deck"]
        query = db.select(
            NwSistEnergia.tb.c["vl_geracao_eol"],
            NwSistEnergia.tb.c["cd_submercado"],
            NwSistEnergia.tb.c["vl_mes"],
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["dt_deck"]

            
        ).where(
            db.and_(
            NwSistEnergia.tb.c["dt_deck"] == last_data_deck,
            db.cast(
                db.func.concat(
                    NwSistEnergia.tb.c["vl_ano"], 
                    '-', 
                    db.func.lpad(NwSistEnergia.tb.c["vl_mes"], 2, '0'), 
                    '-01'
                ).label('data'),
                db.Date
            ).between(start, end)
            )
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=['geracaoEolica', 'codigoSubmercado', 'mes', 'ano', 'dataDeck'])
        return df.to_dict('records')
    

class CvuUsinasTermicas:
    tb:db.Table = __DB__.getSchema('tb_usinas_termicas')
    
    @staticmethod
    def get_usinas_termicas():

        query = db.select(CvuUsinasTermicas.tb)
        rows = __DB__.db_execute(query, commit=prod)
        return pd.DataFrame(rows).to_dict('records')


class Cvu:
    tb:db.Table = __DB__.getSchema('tb_cvu')
    
    @staticmethod
    def create(body: List[CvuSchema]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)
        
        # Drop duplicates based on primary key columns
        primary_key_columns = ['cd_usina', 'tipo_cvu', 'mes_referencia', 'ano_horizonte', 'dt_atualizacao', 'fonte']
        df.drop_duplicates(subset=primary_key_columns, keep='first', inplace=True)
        
        # Delete existing records that match the incoming data
        for _, row in df.iterrows():
            delete_conditions = {
                'cd_usina': row['cd_usina'],
                'tipo_cvu': row['tipo_cvu'],
                'mes_referencia': row['mes_referencia'],
                'ano_horizonte': row['ano_horizonte'],
                'dt_atualizacao': row['dt_atualizacao'],
                'fonte': row['fonte']
            }
            Cvu.delete_by_params(**delete_conditions)
        
        # Insert new records
        logger.info(f"Inserting {len(df)} records into tb_cvu")
        query = db.insert(Cvu.tb).values(df.to_dict('records'))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_cvu")
        return None


    @staticmethod
    def delete_():
        query = db.delete(Cvu.tb).where(Cvu.tb.c.mes_referencia == '202503')
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None

    @staticmethod
    def delete_by_params(**kwargs):
        query = db.delete(Cvu.tb).where(db.and_(*[getattr(Cvu.tb.c, key) == value for key, value in kwargs.items()]))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None

    def get_cvu_by_params_deck(ano_mes_referencia:datetime.date, dt_atualizacao:datetime.date, fonte:str):

        conditions_cvu = []
        conditions_merchant = []

        if ano_mes_referencia is not None:
            mes_referencia = ano_mes_referencia.strftime('%Y%m')
            conditions_cvu.append(Cvu.tb.c.mes_referencia == mes_referencia)
            conditions_merchant.append(CvuMerchant.tb.c.mes_referencia == mes_referencia)
        
        if dt_atualizacao is not None:
            conditions_cvu.append(Cvu.tb.c.dt_atualizacao == dt_atualizacao)
            conditions_merchant.append(CvuMerchant.tb.c.dt_atualizacao == dt_atualizacao)
        
        if fonte is not None:
            conditions_cvu.append(Cvu.tb.c.fonte == fonte)
            conditions_merchant.append(CvuMerchant.tb.c.fonte == fonte)
            
        query = db.select(Cvu.tb).where(db.and_(*conditions_cvu))
        rows = __DB__.db_execute(query)
        df_cvu = pd.DataFrame(rows)

        query = db.select(CvuMerchant.tb).where(db.and_(*conditions_merchant))
        rows = __DB__.db_execute(query)
        df_cvu_merchant = pd.DataFrame(rows)

        if not df_cvu_merchant.empty:
            answer = CvuUsinasTermicas.get_usinas_termicas()
            df_usinas = pd.DataFrame(answer)

            decisao_custo = df_usinas[['cd_usina','flag_custo_fixo']].copy()
            cds_cf = decisao_custo[decisao_custo['flag_custo_fixo'] == 1]['cd_usina'].unique()
            cds_scf = decisao_custo[decisao_custo['flag_custo_fixo'] == 0]['cd_usina'].unique()

            df_merchant_cf = df_cvu_merchant[df_cvu_merchant['cd_usina'].isin(cds_cf)].copy()
            df_merchant_scf = df_cvu_merchant[df_cvu_merchant['cd_usina'].isin(cds_scf)].copy()

            df_aux = df_merchant_cf.drop(columns=['vl_cvu_scf']).rename(columns={'vl_cvu_cf':'vl_cvu'})
            df_cvu_merchant_completo = pd.concat([df_aux ,df_merchant_scf.drop(columns=['vl_cvu_cf']).rename(columns={'vl_cvu_scf':'vl_cvu'})])

            ano_inicial=int(df_cvu_merchant['mes_referencia'].unique()[0][:4])
            
            df_conjuntural = df_cvu_merchant_completo.copy()
            df_conjuntural['tipo_cvu'] = 'conjuntural'
            df_conjuntural['ano_horizonte'] = ano_inicial
            
            df_estrutural = df_cvu_merchant_completo.copy()
            df_estrutural = df_estrutural.loc[df_estrutural.index.repeat(5)].reset_index(drop=True)
            df_estrutural['tipo_cvu'] = 'estrutural'
            ordem_anos_repetidos = list(range(ano_inicial, ano_inicial+5)) * (len(df_estrutural) // 5)
            df_estrutural['ano_horizonte'] = ordem_anos_repetidos

            df_cvu = pd.concat([df_cvu, df_conjuntural, df_estrutural])

        return df_cvu.to_dict('records')


class CvuMerchant:
    tb:db.Table = __DB__.getSchema('tb_cvu_merchant')
    
    @staticmethod
    def create(body: List[CvuMerchantSchema]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)

        unique_params = df[['dt_atualizacao', 'mes_referencia',"fonte"]].drop_duplicates().values
        
        for dt_atualizacao,mes_referencia,fonte in unique_params:
            CvuMerchant.delete_by_params(
                mes_referencia=mes_referencia,
                dt_atualizacao=dt_atualizacao,
                fonte=fonte
                )

        query = db.insert(CvuMerchant.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_cvu")
        return None

    @staticmethod
    def delete_by_params(**kwargs):
        query = db.delete(CvuMerchant.tb).where(db.and_(*[getattr(CvuMerchant.tb.c, key) == value for key, value in kwargs.items()]))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None
    
class CargaSemanalDecomp:
    tb: db.Table = __DB__.getSchema('carga_semanal_dc')
    
    @staticmethod
    def create(body: List[CargaSemanalDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        delete_dates = list(set([x['data_produto'] for x in body_dict]))
        for date in delete_dates:
            CargaSemanalDecomp.delete_by_product_date(date)
        query = db.insert(CargaSemanalDecomp.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na carga_semanal_dc")
        return None
    @staticmethod
    def delete_by_product_date(date:datetime.date):
        query = db.delete(CargaSemanalDecomp.tb).where(CargaSemanalDecomp.tb.c.data_produto == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def get_by_product_date(data_produto:datetime.date):
        query = db.select(
            CargaSemanalDecomp.tb
            ).where(db.and_(
                CargaSemanalDecomp.tb.c.data_produto == data_produto
            ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id','data_produto','semana_operativa','patamar','duracao','submercado','carga','base_cgh','base_eol','base_ufv','base_ute','carga_mmgd','exp_cgh','exp_eol','exp_ufv','exp_ute','estagio'])
        return result.to_dict('records')

class CargaPmo:
    tb: db.Table = __DB__.getSchema('carga_consolidada_pmo')
    
    @staticmethod
    def create(body: List[CargaPmoSchema]):
        # Convert body to list of dictionaries
        body_dict = [x.model_dump() for x in body]
        
        # Extract unique combinations of dt_inicio and revisao
        unique_combinations = []
        for item in body_dict:
            combination = (item['dt_inicio'], item['revisao'])
            if combination not in unique_combinations:
                unique_combinations.append(combination)
        
        # Delete existing records for each unique combination
        for dt_inicio, revisao in unique_combinations:
            CargaPmo.delete_by_dt_inicio_revisao(dt_inicio, revisao)
        
        # Convert dt_inicio from string to datetime
        for item in body_dict:
            item['dt_inicio'] = datetime.datetime.strptime(item['dt_inicio'], '%Y%m%d').date()
        
        # Insert new records
        query = db.insert(CargaPmo.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na carga_consolidada_pmo")
        return {"message": f"{rows} registros de carga PMO inseridos com sucesso"}
    
    @staticmethod
    def delete_by_dt_inicio_revisao(dt_inicio: str, revisao: str):
        # Convert dt_inicio string to date if it's a string
        if isinstance(dt_inicio, str):
            dt_inicio = datetime.datetime.strptime(dt_inicio, '%Y%m%d').date()
            
        # Delete records with matching dt_inicio and revisao
        query = db.delete(CargaPmo.tb).where(
            db.and_(
                CargaPmo.tb.c.dt_inicio == dt_inicio,
                CargaPmo.tb.c.revisao == revisao
            )
        )
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da carga_consolidada_pmo")
        return None
    
    @staticmethod
    def get_by_dt_inicio(dt_inicio: datetime.date, revisao: Optional[str] = None):
        if dt_inicio is None:
            #today
            dt_inicio = datetime.date.today()
        # Build query conditions
        conditions = [CargaPmo.tb.c.dt_inicio == dt_inicio]
        
        # Add revisao condition if provided
        if revisao is not None:
            conditions.append(CargaPmo.tb.c.revisao == revisao)
        
        # Execute query
        query = db.select(CargaPmo.tb).where(db.and_(*conditions))
        result = __DB__.db_execute(query).fetchall()
        
        # Check if empty result
        if not result:
            if revisao:
                raise HTTPException(status_code=404, detail=f"Dados de carga PMO para dt_inicio={dt_inicio} e revisao={revisao} não encontrados")
            else:
                raise HTTPException(status_code=404, detail=f"Dados de carga PMO para dt_inicio={dt_inicio} não encontrados")
        
        # Convert to DataFrame and return as dict
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema', 'semana', 'dt_inicio', 'tipo', 'created_at', 'updated_at']
        df = pd.DataFrame(result, columns=columns)
        return df.to_dict('records')

    @staticmethod
    def get_most_recent_data():
        """
        Obtem os dados mais recentes de carga PMO.
        Busca os 2 períodos mais recentes que já começaram (periodicidade_inicial <= hoje),
        filtrando duplicatas por subsistema+dt_inicio e mantendo apenas registros com carga > 0.
        """
        today = datetime.date.today()
        
        # Step 1: Get the top 2 most recent periods that have already started
        query_periodos = db.select(
            CargaPmo.tb.c.periodicidade_inicial.distinct()
        ).where(
            CargaPmo.tb.c.periodicidade_inicial <= today
        ).order_by(
            CargaPmo.tb.c.periodicidade_inicial.desc()
        ).limit(2)
        
        periodos_result = __DB__.db_execute(query_periodos).fetchall()
        
        if not periodos_result:
            raise HTTPException(status_code=404, detail="Nenhum período válido encontrado")
        
        # Extract the period dates
        periodos_validos = [row[0] for row in periodos_result]
        
        # Step 2: Get all records from these periods
        query_dados = db.select(CargaPmo.tb).where(
            CargaPmo.tb.c.periodicidade_inicial.in_(periodos_validos)
        ).order_by(
            CargaPmo.tb.c.periodicidade_inicial.desc(),
            CargaPmo.tb.c.subsistema,
            CargaPmo.tb.c.dt_inicio,
            CargaPmo.tb.c.id.desc()
        )
        
        result = __DB__.db_execute(query_dados).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="Dados de carga PMO não encontrados")
        
        # Step 3: Convert to DataFrame and apply deduplication logic
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema', 'semana', 'dt_inicio', 'tipo', 
                   'periodicidade_inicial', 'periodicidade_final', 'created_at', 'updated_at']
        df = pd.DataFrame(result, columns=columns)
        
        # Step 4: Apply filters and deduplication
        # Filter out records with carga <= 0
        df = df[df['carga'] > 0]
        
        # Deduplicate: keep only the first row for each subsistema+dt_inicio combination
        # (already ordered by periodicidade_inicial desc, id desc)
        df_deduplicated = df.drop_duplicates(subset=['subsistema', 'dt_inicio'], keep='first')
        
        # Final ordering
        df_final = df_deduplicated.sort_values([
            'periodicidade_inicial', 'subsistema', 'dt_inicio'
        ], ascending=[False, True, True])
        
        df_final['semana'] = df_final['semana'].fillna(0).astype(int)
        print(f"Dados de carga PMO encontrados: {len(df_final)} registros")
        print(df_final.to_string())
        return df_final.to_dict('records')
    
    @staticmethod
    def get_historico_versus_previsao(dt_referencia: datetime.date, revisao: str):
        """
        Obtém os dados históricos (realizados) versus previsões para uma data de referência.
        
        Args:
            dt_referencia: Data de referência para comparação (normalmente a data atual)
            revisao: Número da revisão a ser considerada
            
        Returns:
            Um dicionário com dados históricos e previsões separados
        """
        # Busca todos os registros com a revisão especificada
        query = db.select(CargaPmo.tb).where(
            db.and_(
                CargaPmo.tb.c.revisao == revisao
            )
        ).order_by(CargaPmo.tb.c.dt_inicio, CargaPmo.tb.c.semana)
        
        result = __DB__.db_execute(query).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Dados de carga PMO para revisão {revisao} não encontrados")
        
        # Converte para DataFrame
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema', 'semana', 'dt_inicio', 'tipo', 'created_at', 'updated_at']
        df = pd.DataFrame(result, columns=columns)
        
        # Adiciona coluna de status (realizado/previsto)
        df['status'] = df.apply(
            lambda row: 'realizado' if row['dt_inicio'] < dt_referencia else 'previsto', 
            axis=1
        )
        
        # Separa em histórico e previsão
        historico = df[df['status'] == 'realizado'].to_dict('records')
        previsao = df[df['status'] == 'previsto'].to_dict('records')
        
        return {
            "historico": historico,
            "previsao": previsao,
            "data_referencia": dt_referencia.isoformat()
        }
    
    @staticmethod
    def marcar_realizado_previsto(dados_carga: List[dict], data_referencia: datetime.date):
        """
        Marca os registros como realizados ou previstos com base na data de referência.
        
        Args:
            dados_carga: Lista de dicionários contendo os dados de carga
            data_referencia: Data de referência para comparação
            
        Returns:
            Lista com os dados de carga marcados como realizados ou previstos
        """
        for item in dados_carga:
            dt_inicio = item.get('dt_inicio')
            semana = item.get('semana')
            
            # Verifica se é um registro mensal ou semanal
            if item.get('tipo') == 'mensal':
                item['status'] = 'realizado' if dt_inicio < data_referencia else 'previsto'
            else:
                # Para registros semanais, considera como realizado se for anterior à semana atual
                semanas_passadas_desde_inicio = (data_referencia - dt_inicio).days // 7 + 1
                item['status'] = 'realizado' if semana and semana <= semanas_passadas_desde_inicio else 'previsto'
        
        return dados_carga