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
from .schema import (
    WeolSemanalSchema,
    PatamaresDecompSchema,
    CvuMerchantSchema,
    CvuSchema,
    CargaSemanalDecompSchema,
    CargaPmoSchema,
    CargaNewaveSistemaEnergiaSchema,
    CargaNewaveCadicSchema,
    CheckCvuCreateDto,
    CheckCvuReadDto,
)

logger = logging.getLogger(__name__)


prod = True
__DB__ = db_mysql_master('db_decks')


class WeolSemanal:
    tb: db.Table = __DB__.getSchema('tb_dc_weol_semanal')

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
    def delete_by_product_date(date: datetime.date):
        query = db.delete(WeolSemanal.tb).where(
            WeolSemanal.tb.c.data_produto == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None

    @staticmethod
    def get_all():
        query = db.select(WeolSemanal.tb)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana',
                              'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        result = result.to_dict('records')
        return result

    @staticmethod
    def get_by_product_date_start_week_year_month_rv(data_produto: datetime.date, mes_eletrico: int, ano: int, rv: int):
        query = db.select(
            WeolSemanal.tb
        ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.mes_eletrico == mes_eletrico,
                db.func.year(WeolSemanal.tb.c.inicio_semana) == ano,
                WeolSemanal.tb.c.rv_atual == rv
                ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana',
                              'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')

    @staticmethod
    def get_by_product_start_week_date_product_date(inicio_semana: datetime.date, data_produto: datetime.date):
        query = db.select(
            WeolSemanal.tb
        ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.inicio_semana >= inicio_semana
                ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana',
                              'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')

    @staticmethod
    def get_by_product_date(data_produto: datetime.date):
        query = db.select(
            WeolSemanal.tb
        ).where(WeolSemanal.tb.c.data_produto == data_produto)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana',
                              'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(
                status_code=404, detail=f"Produto da data {data_produto} não encontrado")
        return result.to_dict('records')

    @staticmethod
    def get_by_product_date_between(start_date: datetime.date, end_date: datetime.date):
        query = db.select(
            WeolSemanal.tb
        ).where(WeolSemanal.tb.c.data_produto.between(start_date, end_date))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana',
                              'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(
                status_code=404, detail=f"Produtos entre as datas {start_date} e {end_date} não encontrados")
        return result.to_dict('records')

    @staticmethod
    def get_weighted_avg_by_product_date(product_date: datetime.date, ):
        df = pd.DataFrame(WeolSemanal.get_by_product_date(product_date))
        df_horas_por_patamar = pd.DataFrame(Patamares.get_horas_por_patamar_por_inicio_semana_data(
            df['inicioSemana'].min(), df['finalSemana'].max()))
        merged_df = pd.merge(df, df_horas_por_patamar, on=[
                             'inicioSemana', 'patamar'], how='left')

        df_weighted = merged_df[[
            'dataProduto', 'inicioSemana', 'qtdHoras']][merged_df['submercado'] == "S"]
        df_weighted = df_weighted.groupby(['dataProduto', 'inicioSemana']).agg(
            {'qtdHoras': 'sum'}).rename({'qtdHoras': 'totalHoras'}, axis=1)

        merged_df = pd.merge(df_weighted, merged_df, on=[
                             'dataProduto', 'inicioSemana'], how='left')

        df_group = merged_df[['dataProduto', 'inicioSemana',
                              'patamar', 'valor', 'qtdHoras', 'totalHoras', 'submercado']]

        df_group['mediaPonderada'] = df_group['valor'] * df_group['qtdHoras']

        df_group = df_group.groupby(['dataProduto', 'inicioSemana', 'submercado']).agg(
            {'mediaPonderada': 'sum', 'totalHoras': 'max'}).reset_index()

        df_group['mediaPonderada'] = df_group['mediaPonderada'] / \
            df_group['totalHoras']
        df_group.sort_values(by=['submercado', 'inicioSemana'], inplace=True)

        return df_group.to_dict('records')

    @staticmethod
    def get_weighted_avg_by_product_date_between(start_date: datetime.date, end_date: datetime.date):
        df = pd.DataFrame(
            WeolSemanal.get_by_product_date_between(start_date, end_date))

        df_horas_por_patamar = pd.DataFrame(Patamares.get_horas_por_patamar_por_inicio_semana_data(
            df['inicioSemana'].min(), df['finalSemana'].max()))
        merged_df = pd.merge(df, df_horas_por_patamar, on=[
                             'inicioSemana', 'patamar'], how='left')

        df_weighted = merged_df[[
            'dataProduto', 'inicioSemana', 'qtdHoras']][merged_df['submercado'] == "S"]
        df_weighted = df_weighted.groupby(['dataProduto', 'inicioSemana']).agg(
            {'qtdHoras': 'sum'}).rename({'qtdHoras': 'totalHoras'}, axis=1)

        merged_df = pd.merge(df_weighted, merged_df, on=[
                             'dataProduto', 'inicioSemana'], how='left')
        df_group = merged_df.groupby(['dataProduto', 'inicioSemana', 'patamar']).agg(
            {'valor': 'sum', 'qtdHoras': 'max', 'totalHoras': 'max'}).reset_index()

        df_group['mediaPonderada'] = df_group['valor'] * df_group['qtdHoras']
        df_group = df_group.groupby(['dataProduto', 'inicioSemana']).agg(
            {'mediaPonderada': 'sum', 'totalHoras': 'max'}).reset_index()
        df_group['mediaPonderada'] = df_group['mediaPonderada'] / \
            df_group['totalHoras']

        df = df_group.pivot(index=[
                            'inicioSemana'], columns='dataProduto', values='mediaPonderada').reset_index()
        return df.to_dict('records')

    @staticmethod
    def get_html_weighted_avg(df: pd.DataFrame):
        # Array de 40 cores do verde ao vermelho
        performance_colors = [
            "#00FF00", "#0AFF00", "#14FF00", "#1EFF00", "#28FF00", "#32FF00",
            "#3CFF00", "#46FF00", "#50FF00", "#5AFF00", "#64FF00", "#6EFF00",
            "#78FF00", "#82FF00", "#8CFF00", "#96FF00", "#A0FF00", "#AAFF00",
            "#B4FF00", "#BEFF00", "#C8FF00", "#D2FF00", "#DCFF00", "#E6FF00",
            "#F0FF00", "#FAFF00", "#FFFF00", "#FFFA00", "#FFF000", "#FFE600",
            "#FFDC00", "#FFD200", "#FFC800", "#FFBE00", "#FFB400", "#FFAA00",
            "#FFA000", "#FF9600", "#FF8C00", "#FF8200", "#FF7800", "#FF6E00",
            "#FF6400", "#FF5A00", "#FF5000", "#FF4600", "#FF3C00", "#FF3200",
            "#FF2800", "#FF1E00", "#FF1400", "#FF0A00", "#FF0000"
        ]
        
        # Gerar classes CSS dinamicamente
        css_classes = ""
        for i, color in enumerate(performance_colors):
            css_classes += f".n{i}{{background-color: {color};}}"
        
        html: str = f'''<style> 
            body {{ font-family: sans-serif; }} 
            th, td {{ padding: 4px; text-align: center; border: 0.5px solid #999; }}
            table {{ border-collapse: collapse; }} 
            thead, .gray {{ background-color: #f2f2f2; border: 1px solid #ddd; }} 
            .none{{ background-color: #FFF; }} 
            tbody *{{ border: none; }}
            tbody{{ border: 1px solid #ddd; }} 
            {css_classes}
        </style><table> <thead> <tr>'''
        
        for col in df.columns:
            html += f'<th>{col}</th>'
        html += ' </tr></thead><tbody>'

        eolica_newave = df[df['Origem'] == 'Eolica Newave']
        neutral_index = len(performance_colors) // 2  # Índice do meio (amarelo neutro)

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
                        html += f'<td class="n{neutral_index}">{int(col)}</td>'
                        continue
                    
                    percent_diff: float = col - eolica_newave.iloc[0].iloc[j]
                    
                    color_offset = int(percent_diff / 250)
                    color_index = neutral_index - color_offset
                    
                    color_index = max(0, min(len(performance_colors) - 1, color_index))
                    
                    html += f'<td class="n{color_index}">{int(col)}</td>'

            html += '</tr>'
        html += '</tbody></table>'
        # with open('weol.html', 'w') as f:
        #     f.write(html)
        return {"html": html}

    @staticmethod
    def get_weighted_avg_table_monthly_by_product_date(data_produto: datetime.date, quantidade_produtos: int):
        df = pd.DataFrame(WeolSemanal.get_weighted_avg_by_product_date_between(
            data_produto - datetime.timedelta(days=quantidade_produtos-1), data_produto))
        
        eol_nw_inicio  = ElecData(df['inicioSemana'][0])
        eol_nw_inicio = datetime.date(eol_nw_inicio.anoReferente, eol_nw_inicio.mesReferente, 1)
        eol_nw_fim = ElecData(df['inicioSemana'][len(df['inicioSemana'])-1])
        eol_nw_fim = datetime.date(eol_nw_fim.anoReferente, eol_nw_fim.mesReferente, 1)
        df_eol_newave = pd.DataFrame(NwSistEnergia.get_eol_by_last_data_deck_mes_ano_between(
            eol_nw_inicio, eol_nw_fim
            ))

        df_eol_newave = df_eol_newave.groupby(['mes', 'ano']).agg(
            {'geracaoEolica': 'sum'}).reset_index()
        df_eol_newave['yearMonth'] = df_eol_newave['ano'].astype(
            str) + '-' + df_eol_newave['mes'].astype(str)
        columns_rename = [MONTH_DICT[int(
            row['mes'])] + f' {int(row["ano"])}' for i, row in df_eol_newave.iterrows()]

        df['ano'] = [ElecData(row).anoReferente if type(
            row) is not str else row for row in df['inicioSemana']]
        df['mes'] = [ElecData(row).mesReferente if type(
            row) is not str else row for row in df['inicioSemana']]

        df_eol_newave = df_eol_newave.sort_values(by='yearMonth')
        df_eol_newave.drop(columns=['mes', 'ano'], inplace=True)

        df['yearMonth'] = df['yearMonth'] = df['ano'].astype(str) + '-' + df['mes'].astype(str)

        df.drop(columns=['inicioSemana'], inplace=True)
        df = df.groupby('yearMonth').mean()

        df = pd.merge(df_eol_newave, df, on='yearMonth', how='left')

        df.drop(columns=['mes', 'ano'], inplace=True)

        df['yearMonth'] = columns_rename
        df.rename(columns={'yearMonth': 'Origem',
                  'geracaoEolica': 'Eolica Newave'}, inplace=True)

        df.columns = [df.columns[0]] + [(x + datetime.timedelta(days=1)).strftime(
            'WEOL %d/%m') if type(x) != str else x for x in df.columns[1:]]
        df = df[[df.columns[1], df.columns[0]] + df.columns[2:].to_list()]

        df = df.transpose()
        df.reset_index(inplace=True)
        df.columns = df.iloc[0]
        df = df[1:]

        return WeolSemanal.get_html_weighted_avg(df)

    @staticmethod
    def get_weighted_avg_table_weekly_by_product_date(data_produto: datetime.date, quantidade_produtos: int):
        df = pd.DataFrame(WeolSemanal.get_weighted_avg_by_product_date_between(
            data_produto - datetime.timedelta(days=quantidade_produtos), data_produto))

        eol_nw_inicio = ElecData(df['inicioSemana'][0])
        eol_nw_inicio = datetime.date(eol_nw_inicio.anoReferente, eol_nw_inicio.mesReferente, 1)
        eol_nw_fim = ElecData(df['inicioSemana'][len(df['inicioSemana'])-1])
        eol_nw_fim = datetime.date(eol_nw_fim.anoReferente, eol_nw_fim.mesReferente, 1)
        df_eol_newave = pd.DataFrame(NwSistEnergia.get_eol_by_last_data_deck_mes_ano_between(
            eol_nw_inicio, eol_nw_fim
            ))

        df_eol_newave = df_eol_newave.groupby(['mes', 'ano']).agg(
            {'geracaoEolica': 'sum'}).reset_index()
        df_eol_newave = df_eol_newave.sort_values(by=['ano', 'mes'])

        df['ano'] = [ElecData(row).anoReferente if type(
            row) is not str else row for row in df['inicioSemana']]
        df['mes'] = [ElecData(row).mesReferente if type(
            row) is not str else row for row in df['inicioSemana']]

        df = pd.merge(df_eol_newave, df, on=['ano', 'mes'], how='left')
        df.drop(columns=['mes', 'ano'], inplace=True)

        df.rename(columns={'inicioSemana': 'Origem',
                  'geracaoEolica': 'Eolica Newave'}, inplace=True)

        df.columns = [df.columns[0]] + [(x + datetime.timedelta(days=1)).strftime(
            'WEOL %d/%m') if type(x) != str else x for x in df.columns[1:]]
        df = df[[df.columns[1], df.columns[0]] + df.columns[2:].to_list()]

        df = df.transpose()
        df.reset_index(inplace=True)

        df.columns = df.iloc[0]
        df = df[1:]
        df.drop(columns=[np.nan], inplace=True, errors='ignore')
        df.columns = [df.columns[0]] + [f'{MONTH_DICT[ElecData(x).mesReferente]}-rv{ElecData(x).atualRevisao}' if type(
            x) != str else x for x in df.columns[1:]]
        return WeolSemanal.get_html_weighted_avg(df)

    @staticmethod
    def create_diferenca_table_rv(
        df: pd.DataFrame,
        titulo: str,
        data_diferenca1: datetime.date = None,
        data_diferenca2: datetime.date = None,
    ):
        html = f'''<style>
            table {{ font-family: sans-serif; border-collapse: collapse; }}
            th, td {{ padding: 8px; text-align: center; border: 1px solid #ccc; }}
            th {{ background-color: #f2f2f2; font-weight: bold; }}
            .positive {{ color: #155724;background-color: #d4edda;font-weight: bold;}}
            .negative {{ color: #721c24;background-color: #f8d7da;font-weight: bold;}}
        </style>
        <table>
            <thead>
            <tr>
                <th colspan="2">{titulo} {data_diferenca1.strftime("%d/%m/%Y")} - {data_diferenca2.strftime("%d/%m/%Y")}</th>
            </tr>
            </thead>
            <thead>
                <tr>
                    <th>RV</th>
                    <th>Diferença</th>
                </tr>
            </thead>
            <tbody>'''

        for _, row in df.iterrows():
            rv = row['rv']
            diferenca = row['diferenca']

            if diferenca > 0:
                css_class = 'positive'
            elif diferenca < 0:
                css_class = 'negative'
            else:
                css_class = ''

            html += f'''
                <tr>
                    <td>{rv}</td>
                    <td class="{css_class}">{diferenca:.2f}</td>
                </tr>'''

        html += '''
            </tbody>
        </table>'''

        return html

    @staticmethod
    def get_weol_day_minus_one_diff(data_produto: datetime.date, days_to_subtract: int):
        df_minuendo = pd.DataFrame(
            WeolSemanal.get_weighted_avg_by_product_date_between(
                data_produto, data_produto)
        )
        data_diferenca = data_produto-datetime.timedelta(days=days_to_subtract)
        df_subtraendo = pd.DataFrame()
        while df_subtraendo.empty:
            try:
                df_subtraendo = pd.DataFrame(
                    WeolSemanal.get_weighted_avg_by_product_date_between(
                        data_diferenca,
                        data_diferenca
                    )
                )
            except HTTPException:
                data_diferenca -= datetime.timedelta(days=1)
        df_minuendo.columns = ['inicioSemana', 'valores']
        df_subtraendo.columns = ['inicioSemana', 'valores']
        df_merged = df_minuendo.merge(df_subtraendo, on='inicioSemana', suffixes=('_df1', '_df2'))

        df_merged['diferenca'] = df_merged['valores_df1'] - df_merged['valores_df2']

        df_resultado = df_merged[['inicioSemana', 'diferenca']]
        df_resultado['inicioSemana'] = pd.to_datetime(df_resultado['inicioSemana']).dt.date

        df_resultado['inicioSemana'] = df_resultado['inicioSemana'].apply(
            lambda x: f'{MONTH_DICT[ElecData(x).mesReferente]}-rv{ElecData(x).atualRevisao}' if isinstance(x, datetime.date) else x
        )
        df_resultado.rename(columns={'inicioSemana': 'rv'}, inplace=True)
        
        html = WeolSemanal.create_diferenca_table_rv(df_resultado, 'Diferença WEOL', data_produto+datetime.timedelta(days=1), data_diferenca+datetime.timedelta(days=1))
        # with open('weol_diff.html', 'w') as f:
        #    f.write(html)
            
        return {'html': html}
class Patamares:
    tb: db.Table = __DB__.getSchema('tb_patamar_decomp')

    @staticmethod
    def create(body: List[PatamaresDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        dates = list(set([x['inicio'] for x in body_dict]))
        dates.sort()
        Patamares.delete_by_start_date_between(
            dates[0].date(), dates[-1].date())
        query = db.insert(Patamares.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date(date: datetime.date):
        query = db.delete(Patamares.tb).where(
            db.func.date(Patamares.tb.c.inicio) == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date_between(start: datetime.date, end: datetime.date):
        query = db.delete(Patamares.tb).where(
            db.func.date(Patamares.tb.c.inicio).between(start, end))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None

    @staticmethod
    def get_horas_por_patamar_por_inicio_semana_data(inicio_semana: datetime.date, fim_semana: datetime.date):
        query = db.select(
            db.func.count(),
            Patamares.tb.c["patamar"],
            db.func.min(db.func.date(Patamares.tb.c["inicio"]))
        ).where(
            db.func.date(db.func.date_sub(Patamares.tb.c["inicio"], db.text(
                "interval 1 hour"))).between(inicio_semana, fim_semana)
        ).group_by(
            Patamares.tb.c["semana"],
            Patamares.tb.c["patamar"]
        )
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(
            result, columns=["qtdHoras", "patamar", "inicio"])
        result = result.sort_values(by=["inicio", "patamar"])

        for i in range(2, len(result), 3):
            result.at[i, 'inicio'] = result.at[i-1, 'inicio']

        result.loc[(result['patamar'] != 'Pesada') & (
            result['patamar'] != 'Leve'), 'patamar'] = 'medio'
        result.loc[result['patamar'] == 'Pesada', 'patamar'] = 'pesado'
        result.loc[result['patamar'] == 'Leve', 'patamar'] = 'leve'
        result = result.rename(columns={'inicio': 'inicioSemana'})
        return result.to_dict("records")


class NwSistEnergia:
    tb: db.Table = __DB__.getSchema('tb_nw_sist_energia')

    @staticmethod
    def get_last_data_deck():
        query = db.select(
            db.func.max(NwSistEnergia.tb.c["dt_deck"])
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=['dt_deck'])
        return df.to_dict('records')

    @staticmethod
    def get_eol_by_last_data_deck_mes_ano_between(start: datetime.date, end: datetime.date):
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
        df = pd.DataFrame(result, columns=[
                          'geracaoEolica', 'codigoSubmercado', 'mes', 'ano', 'dataDeck'])
        df = df.sort_values('dataDeck')
        df = df.drop_duplicates(subset=['codigoSubmercado', 'mes', 'ano'], keep='last')
        return df.to_dict('records')

    @staticmethod
    def post_newave_sist_energia(body: List[CargaNewaveSistemaEnergiaSchema]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            # Convert dt_deck from string to date if it's a string
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
                
        # Delete existing records for each unique dt_deck
        unique_dates = list(set([x['dt_deck'] for x in body_dict]))
        for date in unique_dates:
            NwSistEnergia.delete_sist_deck_by_dt_deck(date)
        
        query = db.insert(NwSistEnergia.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        
        return {"message": f"{rows} registros de sistema de energia Newave inseridos com sucesso"}
        
    @staticmethod
    def delete_sist_deck_by_dt_deck(dt_deck:datetime.date):
        
        query = db.delete(NwSistEnergia.tb).where(
            NwSistEnergia.tb.c.dt_deck == dt_deck
        )
        
        rows = __DB__.db_execute(query, commit=prod).rowcount
        
        logger.info(f"{rows} linhas deletadas da tb_nw_sist_energia")
        return None
    
    
    @staticmethod
    def get_sist_unsi_deck_values():
        """
        Obtém os valores de geração dos dois decks mais recentes, agrupados por deck,
        mês e ano, sem agrupamento por submercado.

        Returns:
            Lista com informações dos dois decks mais recentes, contendo dados
            de geração, organizados por mês e ano.
        """
        subquery = db.select(
            NwSistEnergia.tb.c["dt_deck"]
        ).distinct().order_by(
            NwSistEnergia.tb.c["dt_deck"].desc()
        ).limit(2).alias('latest_decks')

        query = db.select(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_mes"],
            NwSistEnergia.tb.c["vl_ano"],
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_pch"]).label("vl_geracao_pch"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_pct"]).label("vl_geracao_pct"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_eol"]).label("vl_geracao_eol"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_ufv"]).label("vl_geracao_ufv")
        ).where(
            NwSistEnergia.tb.c["dt_deck"].in_(db.select(subquery.c.dt_deck))
        ).group_by(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NwSistEnergia.tb.c["dt_deck"].desc(),
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        )

        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'vl_mes', 'vl_ano', 'PCH', 'PCT', 'EOL', 'UFV'
        ])

        if df.empty:
            return []

        decks_data = []
        for dt_deck, deck_group in df.groupby('dt_deck'):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "data": []
            }

            for (ano, mes), month_group in deck_group.groupby(['vl_ano', 'vl_mes']):
                row = month_group.iloc[0]

                total_geracao = float(row['PCH']) + float(row['PCT']) + float(row['EOL']) + float(row['UFV'])
            
                data = {
                    "vl_mes": int(mes),
                    "vl_ano": int(ano),
                    "vl_deck_unsi": total_geracao
                }

                deck_info["data"].append(data)

            decks_data.append(deck_info)

        return decks_data
    
    @staticmethod
    def get_sist_mmgd_expansao_deck_values():
        
        subquery = db.select(
            NwSistEnergia.tb.c["dt_deck"]
        ).distinct().order_by(
            NwSistEnergia.tb.c["dt_deck"].desc()
        ).limit(2).alias('latest_decks')

        query = db.select(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_mes"],
            NwSistEnergia.tb.c["vl_ano"],
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_pch_mmgd"]).label("vl_geracao_pch_mmgd"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_pct_mmgd"]).label("vl_geracao_pct_mmgd"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_eol_mmgd"]).label("vl_geracao_eol_mmgd"),
            db.func.sum(NwSistEnergia.tb.c["vl_geracao_ufv_mmgd"]).label("vl_geracao_uf_mmgd")
        ).where(
            NwSistEnergia.tb.c["dt_deck"].in_(db.select(subquery.c.dt_deck))
        ).group_by(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NwSistEnergia.tb.c["dt_deck"].desc(),
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        )

        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'vl_mes', 'vl_ano', 'PCH_MMGD', 'PCT_MMGD', 'EOL_MMGD', 'UFV_MMGD'
        ])
        
        if df.empty:
            return []

        decks_data = []
        for dt_deck, deck_group in df.groupby('dt_deck'):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "data": []
            }

            for (ano, mes), month_group in deck_group.groupby(['vl_ano', 'vl_mes']):
                row = month_group.iloc[0]

                total_geracao = float(row['PCH_MMGD']) + float(row['PCT_MMGD']) + float(row['EOL_MMGD']) + float(row['UFV_MMGD'])

                data = {
                    "vl_mes": int(mes),
                    "vl_ano": int(ano),
                    "vl_deck_mmgd_exp": total_geracao
                }

                deck_info["data"].append(data)

            decks_data.append(deck_info)
        
        return decks_data
    
    @staticmethod
    def get_sist_carga_global_deck_values():
        
        # Get MMGD total values
        mmgd_base_values = NewaveCadic.get_sist_mmgd_base_deck_values()
        
        subquery = db.select(
            NwSistEnergia.tb.c["dt_deck"]
        ).distinct().order_by(
            NwSistEnergia.tb.c["dt_deck"].desc()
        ).limit(2).alias('latest_decks')

        query = db.select(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_mes"],
            NwSistEnergia.tb.c["vl_ano"],
            db.func.sum(NwSistEnergia.tb.c["vl_energia_total"]).label("vl_energia_total")
        ).where(
            NwSistEnergia.tb.c["dt_deck"].in_(db.select(subquery.c.dt_deck))
        ).group_by(
            NwSistEnergia.tb.c["dt_deck"],
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NwSistEnergia.tb.c["dt_deck"].desc(),
            NwSistEnergia.tb.c["vl_ano"],
            NwSistEnergia.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'vl_mes', 'vl_ano', 'vl_deck_energia_total'
        ])
        
        if df.empty:
            return []
        
        # Criando um dicionário para relacionar os valores de MMGD total com energia total
        mmgd_dict = {}
        for deck in mmgd_base_values:
            dt_deck = deck.get('dt_deck')
            mmgd_dict[dt_deck] = {}
            
            for item in deck.get('data', []):
                ano = item.get('vl_ano')
                mes = item.get('vl_mes')
                
                if ano not in mmgd_dict[dt_deck]:
                    mmgd_dict[dt_deck][ano] = {}
                    
                mmgd_dict[dt_deck][ano][mes] = item.get('vl_deck_mmgd_base', 0)
        
        # Adicionar coluna de MMGD total baseado no dicionário
        df['vl_deck_mmgd_base'] = df.apply(
            lambda row: mmgd_dict.get(
                row['dt_deck'].strftime('%Y-%m-%d') if isinstance(row['dt_deck'], datetime.date) else str(row['dt_deck']), 
                {}
            ).get(
                row['vl_ano'], 
                {}
            ).get(
                row['vl_mes'], 
                0
            ),
            axis=1
        )
        
        # Calculando a carga global (soma da energia total e MMGD total)
        df['vl_deck_carga_global'] = df['vl_deck_energia_total'] + df['vl_deck_mmgd_base']
        
        # Formatando o resultado final
        decks_data = []
        for dt_deck, deck_group in df.groupby('dt_deck'):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "data": []
            }
            
            for _, row in deck_group.iterrows():
                data = {
                    "vl_mes": int(row['vl_mes']),
                    "vl_ano": int(row['vl_ano']),
                    "vl_deck_carga_global": float(row['vl_deck_carga_global'])
                }
                deck_info["data"].append(data)
            
            decks_data.append(deck_info)
        
        return decks_data
    
    @staticmethod
    def get_sist_carga_liquida_deck_values():
        """
        Calcula os valores de Carga Líquida (diferença entre Carga Global, MMGD Total e Usinas não simuladas)
        para os dois decks mais recentes.
        
        Returns:
            Lista com informações dos dois decks mais recentes, contendo dados
            de Carga Líquida, organizados por mês e ano.
        """
        # Obter dados de Carga Global, MMGD Total e UNSI
        carga_global_values = NwSistEnergia.get_sist_carga_global_deck_values()
        mmgd_total_values = NwSistEnergia.get_sist_mmgd_total_deck_values()
        unsi_values = NwSistEnergia.get_sist_unsi_deck_values()
        
        if not carga_global_values or not mmgd_total_values or not unsi_values:
            return []
            
        carga_liquida_values = []
        
        for deck_global in carga_global_values:
            dt_deck = deck_global.get('dt_deck')
            
            # Encontrar os decks correspondentes para MMGD e UNSI
            deck_mmgd = None
            deck_unsi = None
            
            for deck in mmgd_total_values:
                if deck.get('dt_deck') == dt_deck:
                    deck_mmgd = deck
                    break
                    
            for deck in unsi_values:
                if deck.get('dt_deck') == dt_deck:
                    deck_unsi = deck
                    break
            
            if deck_mmgd and deck_unsi:
                # Criar dicionários para facilitar o acesso aos valores por (ano, mes)
                mmgd_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_deck_mmgd_total', 0) 
                           for item in deck_mmgd.get('data', [])}
                           
                unsi_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_deck_unsi', 0) 
                           for item in deck_unsi.get('data', [])}
                
                deck_liquida = {
                    "dt_deck": dt_deck,
                    "data": []
                }
                
                for item_global in deck_global.get('data', []):
                    ano = item_global.get('vl_ano')
                    mes = item_global.get('vl_mes')
                    valor_global = item_global.get('vl_deck_carga_global', 0)
                    
                    # Obter valores correspondentes de MMGD e UNSI
                    valor_mmgd = mmgd_dict.get((ano, mes), 0)
                    valor_unsi = unsi_dict.get((ano, mes), 0)
                    
                    # Calcular Carga Líquida = Carga Global - MMGD Total - UNSI
                    carga_liquida = valor_global - valor_mmgd - valor_unsi
                    
                    item_liquida = {
                        "vl_ano": ano,
                        "vl_mes": mes,
                        "vl_deck_carga_liquida": int(round(carga_liquida))
                    }
                    
                    deck_liquida['data'].append(item_liquida)
                
                carga_liquida_values.append(deck_liquida)
        
        return carga_liquida_values
    
    @staticmethod
    def get_sist_mmgd_total_deck_values():
        """
        Calcula os valores totais de MMGD (soma de MMGD base e MMGD expansão)
        para os dois decks mais recentes.
        
        Returns:
            Lista com informações dos decks, contendo dados agregados de MMGD total
            (MMGD base + MMGD expansão), organizados por mês e ano.
        """
        # Obter dados do MMGD base e expansão
        from . import service  # Importação local para evitar referência circular
        mmgd_base_values = service.NewaveCadic.get_sist_mmgd_base_deck_values()
        mmgd_exp_values = NwSistEnergia.get_sist_mmgd_expansao_deck_values()
        
        if not mmgd_base_values or not mmgd_exp_values:
            return []
            
        mmgd_total_values = []
        
        for deck_base in mmgd_base_values:
            dt_deck_base = deck_base.get('dt_deck')
            
            deck_exp = None
            for deck in mmgd_exp_values:
                if deck.get('dt_deck') == dt_deck_base:
                    deck_exp = deck
                    break
            
            if deck_exp:
                exp_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_deck_mmgd_exp', 0) 
                           for item in deck_exp.get('data', [])}
                
                deck_total = {
                    "dt_deck": dt_deck_base,
                    "data": []
                }
                
                for item_base in deck_base.get('data', []):
                    ano = item_base.get('vl_ano')
                    mes = item_base.get('vl_mes')
                    valor_base = item_base.get('vl_deck_mmgd_base', 0)
                    valor_exp = exp_dict.get((ano, mes), 0)
                    
                    item_total = {
                        "vl_ano": ano,
                        "vl_mes": mes,
                        "vl_deck_mmgd_total": int(round(valor_base + valor_exp))
                    }
                    
                    deck_total['data'].append(item_total)
                
                mmgd_total_values.append(deck_total)
        
        return mmgd_total_values

class CvuUsinasTermicas:
    tb: db.Table = __DB__.getSchema('tb_usinas_termicas')

    @staticmethod
    def get_usinas_termicas():

        query = db.select(CvuUsinasTermicas.tb)
        rows = __DB__.db_execute(query, commit=prod)
        return pd.DataFrame(rows).to_dict('records')


class Cvu:
    tb: db.Table = __DB__.getSchema('tb_cvu')

    @staticmethod
    def create(body: List[CvuSchema]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)

        # Drop duplicates based on primary key columns
        primary_key_columns = [
            'cd_usina', 'tipo_cvu', 'mes_referencia', 'ano_horizonte', 'dt_atualizacao', 'fonte']
        df.drop_duplicates(subset=primary_key_columns,
                           keep='first', inplace=True)

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
        query = db.delete(Cvu.tb).where(
            db.and_(*[getattr(Cvu.tb.c, key) == value for key, value in kwargs.items()]))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None

    def get_cvu_by_params_deck(ano_mes_referencia: datetime.date, dt_atualizacao: datetime.date, fonte: str):

        conditions_cvu = []
        conditions_merchant = []

        if ano_mes_referencia is not None:
            mes_referencia = ano_mes_referencia.strftime('%Y%m')
            conditions_cvu.append(Cvu.tb.c.mes_referencia == mes_referencia)
            conditions_merchant.append(
                CvuMerchant.tb.c.mes_referencia == mes_referencia)

        if dt_atualizacao is not None:
            conditions_cvu.append(Cvu.tb.c.dt_atualizacao == dt_atualizacao)
            conditions_merchant.append(
                CvuMerchant.tb.c.dt_atualizacao == dt_atualizacao)

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
            df_cvu_merchant['vl_cvu_cf'] = df_cvu_merchant['vl_cvu_cf'].fillna(df_cvu_merchant['vl_cvu_scf'])

            df_cvu_merchant['vl_cvu'] = np.where(
                df_cvu_merchant['recuperacao_custo_fixo'].str.lower() == 'não',
                df_cvu_merchant['vl_cvu_cf'],
                df_cvu_merchant['vl_cvu_scf']
            )

            df_cvu_merchant_completo = df_cvu_merchant.drop(
                columns=['vl_cvu_cf', 'vl_cvu_scf'])

            ano_inicial = int(
                df_cvu_merchant['mes_referencia'].unique()[0][:4])

            df_conjuntural = df_cvu_merchant_completo.copy()
            df_conjuntural['tipo_cvu'] = 'conjuntural'
            df_conjuntural['ano_horizonte'] = ano_inicial

            df_estrutural = df_cvu_merchant_completo.copy()
            df_estrutural = df_estrutural.loc[df_estrutural.index.repeat(
                5)].reset_index(drop=True)
            df_estrutural['tipo_cvu'] = 'estrutural'
            ordem_anos_repetidos = list(
                range(ano_inicial, ano_inicial+5)) * (len(df_estrutural) // 5)
            df_estrutural['ano_horizonte'] = ordem_anos_repetidos

            df_cvu = pd.concat([df_cvu, df_conjuntural, df_estrutural])
        df_cvu = df_cvu.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df_cvu.to_dict('records')


class CvuMerchant:
    tb: db.Table = __DB__.getSchema('tb_cvu_merchant')

    @staticmethod
    def create(body: List[CvuMerchantSchema]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)

        unique_params = df[['dt_atualizacao',
                            'mes_referencia', "fonte"]].drop_duplicates().values

        for dt_atualizacao, mes_referencia, fonte in unique_params:
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
        query = db.delete(CvuMerchant.tb).where(db.and_(
            *[getattr(CvuMerchant.tb.c, key) == value for key, value in kwargs.items()]))
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
    def delete_by_product_date(date: datetime.date):
        query = db.delete(CargaSemanalDecomp.tb).where(
            CargaSemanalDecomp.tb.c.data_produto == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None

    @staticmethod
    def get_by_product_date(data_produto: datetime.date):
        query = db.select(
            CargaSemanalDecomp.tb
        ).where(db.and_(
                CargaSemanalDecomp.tb.c.data_produto == data_produto
                ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'data_produto', 'semana_operativa', 'patamar', 'duracao', 'submercado', 'carga',
                              'base_cgh', 'base_eol', 'base_ufv', 'base_ute', 'carga_mmgd', 'exp_cgh', 'exp_eol', 'exp_ufv', 'exp_ute', 'estagio'])
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
            item['dt_inicio'] = datetime.datetime.strptime(
                item['dt_inicio'], '%Y%m%d').date()

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
            # today
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
                raise HTTPException(
                    status_code=404, detail=f"Dados de carga PMO para dt_inicio={dt_inicio} e revisao={revisao} não encontrados")
            else:
                raise HTTPException(
                    status_code=404, detail=f"Dados de carga PMO para dt_inicio={dt_inicio} não encontrados")

        # Convert to DataFrame and return as dict
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema',
                   'semana', 'dt_inicio', 'tipo', 'created_at', 'updated_at']
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
            raise HTTPException(
                status_code=404, detail="Nenhum período válido encontrado")

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
            raise HTTPException(
                status_code=404, detail="Dados de carga PMO não encontrados")

        # Step 3: Convert to DataFrame and apply deduplication logic
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema', 'semana', 'dt_inicio', 'tipo',
                   'periodicidade_inicial', 'periodicidade_final', 'created_at', 'updated_at']
        df = pd.DataFrame(result, columns=columns)

        # Step 4: Apply filters and deduplication
        # Filter out records with carga <= 0
        df = df[df['carga'] > 0]

        # Deduplicate: keep only the first row for each subsistema+dt_inicio combination
        # (already ordered by periodicidade_inicial desc, id desc)
        df_deduplicated = df.drop_duplicates(
            subset=['subsistema', 'dt_inicio'], keep='first')

        # Final ordering
        df_final = df_deduplicated.sort_values([
            'periodicidade_inicial', 'subsistema', 'dt_inicio'
        ], ascending=[False, True, True])

        df_final['semana'] = df_final['semana'].fillna(0).astype(int)
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
            raise HTTPException(
                status_code=404, detail=f"Dados de carga PMO para revisão {revisao} não encontrados")

        # Converte para DataFrame
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema',
                   'semana', 'dt_inicio', 'tipo', 'created_at', 'updated_at']
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
                semanas_passadas_desde_inicio = (
                    data_referencia - dt_inicio).days // 7 + 1
                item['status'] = 'realizado' if semana and semana <= semanas_passadas_desde_inicio else 'previsto'

        return dados_carga
    


class NewaveCadic:
    tb:db.Table = __DB__.getSchema('tb_nw_cadic')
    
    @staticmethod
    def post_newave_cadic(body: List[CargaNewaveCadicSchema]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            # Convert dt_deck from string to date if it's a string
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
                
        # Delete existing records for each unique dt_deck
        unique_dates = list(set([x['dt_deck'] for x in body_dict]))
        for date in unique_dates:
            NewaveCadic.delete_cadic_deck_by_dt_deck(date)
        
        query = db.insert(NewaveCadic.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        
        return {"message": f"{rows} registros de sistema de energia Newave inseridos com sucesso"}
        
    @staticmethod
    def delete_cadic_deck_by_dt_deck(dt_deck:datetime.date):
        query = db.delete(NewaveCadic.tb).where(
            NewaveCadic.tb.c.dt_deck == dt_deck
        )
        
        rows = __DB__.db_execute(query, commit=prod).rowcount
        
        logger.info(f"{rows} linhas deletadas da tb_nw_sist_energia")
        return None
    
    @staticmethod
    def get_sist_mmgd_base_deck_values():
        
        subquery = db.select(
            NewaveCadic.tb.c["dt_deck"]
        ).distinct().order_by(
            NewaveCadic.tb.c["dt_deck"].desc()
        ).limit(2).alias('latest_decks')

        query = db.select(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["vl_mes"],
            NewaveCadic.tb.c["vl_ano"],
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_se"]).label("vl_mmgd_se"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_s"]).label("vl_mmgd_s"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_ne"]).label("vl_mmgd_ne"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_n"]).label("vl_mmgd_n")
            
        ).where(
            NewaveCadic.tb.c["dt_deck"].in_(db.select(subquery.c.dt_deck))
        ).group_by(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        ).order_by(
            NewaveCadic.tb.c["dt_deck"].desc(),
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'vl_mes', 'vl_ano', 'MMGD_SE', 'MMGD_S', 'MMGD_NE', 'MMGD_N'
        ])

        if df.empty:
            return []

        decks_data = []
        for dt_deck, deck_group in df.groupby('dt_deck'):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "data": []
            }

            for (ano, mes), month_group in deck_group.groupby(['vl_ano', 'vl_mes']):
                row = month_group.iloc[0]

                total_geracao = float(row['MMGD_SE']) + float(row['MMGD_S']) + float(row['MMGD_NE']) + float(row['MMGD_N'])
            
                data = {
                    "vl_mes": int(mes),
                    "vl_ano": int(ano),
                    "vl_deck_mmgd_base": total_geracao
                }

                deck_info["data"].append(data)

            decks_data.append(deck_info)

        return decks_data
    
    


class CheckCvu:
    tb: db.Table = __DB__.getSchema('check_cvu')

    @staticmethod
    def create(body: CheckCvuCreateDto) -> CheckCvuReadDto:
        create_data = body.model_dump(exclude_none=True)

        query = db.insert(CheckCvu.tb).values(**create_data)
        result = __DB__.db_execute(query, commit=prod)

        record_id = result.inserted_primary_key[0]

        select_query = db.select(CheckCvu.tb).where(
            CheckCvu.tb.c.id == record_id)
        created_record = __DB__.db_execute(select_query).fetchone()

        if not created_record:
            raise Exception("Erro ao criar registro de check cvu")

        record_dict = dict(created_record._mapping)
        read_dto = CheckCvuReadDto(**record_dict)

        logger.info(f"Novo cvu criado: {record_id}")
        return read_dto

    def get_by_id(check_cvu_id: int) -> CheckCvuReadDto:
        select_query = db.select(CheckCvu.tb).where(
            CheckCvu.tb.c.id == check_cvu_id)
        record = __DB__.db_execute(select_query).fetchone()

        if not record:
            raise HTTPException(
                status_code=404, detail=f"Check CVU com ID {check_cvu_id} não encontrado")
        record_dict = dict(record._mapping)
        return CheckCvuReadDto(**record_dict)

    @staticmethod
    def update_status_by_id(check_cvu_id: int, status: str) -> CheckCvuReadDto:
        CheckCvu.get_by_id(check_cvu_id)

        update_query = db.update(CheckCvu.tb).where(
            CheckCvu.tb.c.id == check_cvu_id
        ).values(status=status)

        result = __DB__.db_execute(update_query, commit=prod)

        if result.rowcount == 0:
            raise Exception("Erro ao atualizar status do check cvu")

        logger.info(
            f"Status do check cvu ID {check_cvu_id} atualizado para: {status}")
        return CheckCvu.get_by_id(check_cvu_id)

    @staticmethod
    def get_by_data_atualizacao_title(data_atualizacao: datetime.datetime, title: str) -> CheckCvuReadDto:
        select_query = db.select(
            CheckCvu.tb
        ).where(db.and_(
                CheckCvu.tb.c['tipo_cvu'] == title,
                CheckCvu.tb.c['data_atualizacao'] == data_atualizacao))
        record = __DB__.db_execute(select_query).fetchone()

        if not record:
            raise HTTPException(
                status_code=404, detail=f"Check CVU com data de atualizacao {data_atualizacao} não encontrado")
        record_dict = dict(record._mapping)
        return CheckCvuReadDto(**record_dict)

    @staticmethod
    def get_all(
        page: int,
        page_size: int
    ) -> dict:
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 1

        offset = (page - 1) * page_size

        count_query = db.select(db.func.count()).select_from(CheckCvu.tb)
        total_records = __DB__.db_execute(count_query).scalar()

        select_query = db.select(CheckCvu.tb).limit(page_size).offset(offset)
        records = __DB__.db_execute(select_query).fetchall()

        check_cvus = []
        for record in records:
            record_dict = dict(record._mapping)
            check_cvus.append(CheckCvuReadDto(**record_dict).model_dump())

        total_pages = (total_records + page_size - 1) // page_size
        has_next = page < total_pages
        has_previous = page > 1

        return {
            "data": check_cvus,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_previous": has_previous,
                "next_page": page + 1 if has_next else None,
                "previous_page": page - 1 if has_previous else None
            }
        }
