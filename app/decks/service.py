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
    CargaNewaveSistemaEnergiaCreateDto,
    CargaNewaveCadicCreateDto,
    CargaNewaveCadicReadDto,
    CargaNewaveCadicUpdateDto,
    CheckCvuCreateDto,
    CheckCvuReadDto,
    TipoCvuEnum,
    IndiceBlocoEnum,
    PatamaresEnum,
    SubmercadosEnum,
    NewavePatamarCargaUsinaSchema,
    NewavePatamarIntercambioSchema,
    MMGDTotalUpdateDto,
    RestricoesEletricasSchema,
    NewavePrevisoesCargasCreateDto,
    DadosHidraulicosUheCreateDto,
    DadosHidraulicosSubsistemaCreateDto,
)

logger = logging.getLogger(__name__)


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
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_dc_weol_semanal")
        return None

    @staticmethod
    def delete_by_product_date(date: datetime.date):
        query = db.delete(WeolSemanal.tb).where(
            WeolSemanal.tb.c.data_produto == date)
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def get_last_deck_date_weol():
        
        query = db.select(
            db.func.max(WeolSemanal.tb.c.data_produto)
        )
        
        result = __DB__.db_execute(query).fetchone()
        
        return result[0]

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
        if data_produto is None:
            data_produto = WeolSemanal.get_last_deck_date_weol()
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
    def get_weighted_avg_by_product_date(product_date: datetime.date,):
        if product_date is None:
            product_date = WeolSemanal.get_last_deck_date_weol()
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
        df_eol_newave = pd.DataFrame(NewaveSistEnergia.get_eol_by_last_data_deck_mes_ano_between(
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
        df.sort_values(['ano', 'mes'], inplace=True)

        df.drop(columns=['mes', 'ano'], inplace=True)
        df.sort_values('yearMonth', ascending=True)
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
        df_eol_newave = pd.DataFrame(NewaveSistEnergia.get_eol_by_last_data_deck_mes_ano_between(
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
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date(date: datetime.date):
        query = db.delete(Patamares.tb).where(
            db.func.date(Patamares.tb.c.inicio) == date)
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date_between(start: datetime.date, end: datetime.date):
        query = db.delete(Patamares.tb).where(
            db.func.date(Patamares.tb.c.inicio).between(start, end))
        rows = __DB__.db_execute(query).rowcount
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

class CvuUsinasTermicas:
    tb: db.Table = __DB__.getSchema('tb_usinas_termicas')

    @staticmethod
    def get_usinas_termicas():

        query = db.select(CvuUsinasTermicas.tb)
        rows = __DB__.db_execute(query)
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
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_cvu")
        return None

    @staticmethod
    def delete_():
        query = db.delete(Cvu.tb).where(Cvu.tb.c.mes_referencia == '202503')
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None

    @staticmethod
    def delete_by_params(**kwargs):
        query = db.delete(Cvu.tb).where(
            db.and_(*[getattr(Cvu.tb.c, key) == value for key, value in kwargs.items()]))
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None


    @staticmethod
    def get_last_data_atualizacao_por_tipo_cvu(tipo_cvu: str):
        if "merchant" in tipo_cvu.lower():
            query = db.select(
                db.func.max(CvuMerchant.tb.c.dt_atualizacao)
            ).where(
                CvuMerchant.tb.c.fonte == tipo_cvu
            )
        else:
            query = db.select(
                db.func.max(Cvu.tb.c.dt_atualizacao)
            ).where(
                Cvu.tb.c.fonte == tipo_cvu
            )
        __DB__.db_execute(query)
        result = __DB__.db_execute(query).fetchone()
        if result is not None:
            return result[0]
    
    @staticmethod
    def get_cvu_merchant(conditions: list) -> pd.DataFrame:
        query = db.select(CvuMerchant.tb).where(db.and_(*conditions))
        rows = __DB__.db_execute(query)
        df_cvu_merchant = pd.DataFrame(rows)
        
        
        if not df_cvu_merchant.empty:
            df_usinas_cf = pd.DataFrame(CvuMerchantRecuperacaoCustoFixo.get_all())
            df_cvu_merchant['vl_cvu_cf'] = df_cvu_merchant['vl_cvu_cf'].fillna(df_cvu_merchant['vl_cvu_scf'])

            df_cvu_merchant['vl_cvu'] = np.where(
                df_cvu_merchant['recuperacao_custo_fixo'].str.lower() == 'não',
                df_cvu_merchant['vl_cvu_cf'],
                df_cvu_merchant['vl_cvu_scf']
            )

            ano_inicial = int(
                df_cvu_merchant['mes_referencia'].unique()[0][:4])

            df_conjuntural = df_cvu_merchant.copy()
            df_conjuntural['tipo_cvu'] = 'conjuntural'
            df_conjuntural['ano_horizonte'] = ano_inicial
            
            # definindo valores de merchant da data fim pra frente como sem custo fixo
            # essa regra pode ser alterada futuramente
            df_conjuntural['data_fim_aux'] = df_conjuntural['data_fim']
            df_conjuntural.merge(df_usinas_cf, on='cd_usina', how='inner')
            
            df_conjuntural_update_dt_fim = df_conjuntural[df_conjuntural['cd_usina'].isin(
                df_usinas_cf['cd_usina']
            )].merge(df_usinas_cf, on='cd_usina', how='left')
            df_conjuntural_update_dt_fim['data_fim'] = df_conjuntural_update_dt_fim['dt_recupera_cf']
            df_conjuntural[df_conjuntural['cd_usina'].isin(
                df_usinas_cf['cd_usina']
            )] = df_conjuntural_update_dt_fim
            df_scf = df_conjuntural.copy()
            
            df_scf = df_scf[df_scf['cd_usina'].isin(df_usinas_cf['cd_usina'])]
            df_merged = df_scf.merge(df_usinas_cf, on='cd_usina', how='inner')
            df_merged['dt_recupera_cf'] = pd.to_datetime(df_merged['dt_recupera_cf'])
            df_filtrado = df_merged[df_merged['data_fim_aux'] > df_merged['dt_recupera_cf']]
            df_scf = df_filtrado[df_scf.columns]
            
            df_scf['data_inicio'] = df_scf['data_fim']
            df_scf['data_fim'] = df_scf['data_fim_aux']
            df_scf['vl_cvu'] = df_scf['vl_cvu_scf']
            
            df_cvu_merchant = pd.concat([
                df_conjuntural, df_scf
            ], ignore_index=True)
            df_cvu_merchant.drop(columns=['vl_cvu_cf', 'vl_cvu_scf', 'recuperacao_custo_fixo', 'data_fim_aux'], inplace=True)
            df_cvu_merchant.drop_duplicates(['mes_referencia', 'cd_usina', 'data_inicio'], inplace=True)
            df_cvu_merchant = df_cvu_merchant.copy().replace({np.nan: None, np.inf: None, -np.inf: None})
            df_cvu_merchant = df_cvu_merchant.dropna()
            df_cvu_merchant['cd_usina'] = df_cvu_merchant['cd_usina'].astype(int)
            df_cvu_merchant['ano_horizonte'] = df_cvu_merchant['ano_horizonte'].astype(int)
        return df_cvu_merchant
    
    @staticmethod
    def get_cvu_by_tipo_data_atualizacao(
        tipo_cvu: str,
        data_atualizacao: datetime.date
    ):
        conditions_cvu = []
        conditions_merchant = []
        
        if tipo_cvu is None and data_atualizacao is None:
            for tipo_cvu in ['CCEE_conjuntural', 'CCEE_estrutural', 'CCEE_conjuntural_revisado']:
                data_atualizacao = Cvu.get_last_data_atualizacao_por_tipo_cvu(tipo_cvu)
                conditions_cvu.append(
                    db.and_(
                        Cvu.tb.c.fonte == tipo_cvu,
                        Cvu.tb.c.dt_atualizacao == data_atualizacao
                    )
                )
            conditions_cvu = [db.or_(*conditions_cvu)]
            data_atualizacao = Cvu.get_last_data_atualizacao_por_tipo_cvu('CCEE_merchant')
            conditions_merchant.append(
                db.and_(
                    CvuMerchant.tb.c.dt_atualizacao == data_atualizacao
                )
            )
        else:
            if tipo_cvu is not None and tipo_cvu[:5] != "CCEE_":
                tipo_cvu = "CCEE_" + tipo_cvu

            if data_atualizacao is None:
                data_atualizacao = Cvu.get_last_data_atualizacao_por_tipo_cvu(tipo_cvu)

            conditions_cvu.append(Cvu.tb.c.dt_atualizacao == data_atualizacao)
            conditions_merchant.append(
                CvuMerchant.tb.c.dt_atualizacao == data_atualizacao)
            

            if tipo_cvu is not None:
                conditions_cvu.append(Cvu.tb.c.fonte == tipo_cvu)
                conditions_merchant.append(CvuMerchant.tb.c.fonte == tipo_cvu)
        query = db.select(Cvu.tb).where(db.and_(*conditions_cvu))
        rows = __DB__.db_execute(query)
        df_cvu = pd.DataFrame(rows)


        df_cvu_merchant = Cvu.get_cvu_merchant(conditions_merchant)

        if not df_cvu_merchant.empty:
            df_cvu = pd.concat([df_cvu, df_cvu_merchant])
            
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
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_cvu")
        return None

    @staticmethod
    def delete_by_params(**kwargs):
        query = db.delete(CvuMerchant.tb).where(db.and_(
            *[getattr(CvuMerchant.tb.c, key) == value for key, value in kwargs.items()]))
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_cvu")
        return None

class CvuMerchantRecuperacaoCustoFixo:
    tb: db.Table = __DB__.getSchema('cvu_merchant_recupera_cf')

    @staticmethod
    def get_all():
        query = db.select(CvuMerchantRecuperacaoCustoFixo.tb)
        rows = __DB__.db_execute(query).fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=['cd_usina', 'dt_recupera_cf'])
            return df.to_dict('records')
        
        logger.warning("Nenhum dado encontrado na tabela cvu_merchant_recupera_cf")
        raise HTTPException(
            status_code=404, detail="Nenhum dado encontrado na tabela cvu_merchant_recupera_cf")

class CargaSemanalDecomp:
    tb: db.Table = __DB__.getSchema('carga_semanal_dc')

    @staticmethod
    def create(body: List[CargaSemanalDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        delete_dates = list(set([x['data_produto'] for x in body_dict]))
        for date in delete_dates:
            CargaSemanalDecomp.delete_by_product_date(date)
        query = db.insert(CargaSemanalDecomp.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na carga_semanal_dc")
        return None

    @staticmethod
    def delete_by_product_date(date: datetime.date):
        query = db.delete(CargaSemanalDecomp.tb).where(
            CargaSemanalDecomp.tb.c.data_produto == date)
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None


    @staticmethod
    def get_last_date():
        query = db.select(
            db.func.max(CargaSemanalDecomp.tb.c.data_produto)
        )
        result = __DB__.db_execute(query).fetchone()
        if result and result[0]:
            return result[0]
        else:
            raise HTTPException(
                status_code=404, detail="Nenhum dado de carga semanal DECOMP encontrado")

    @staticmethod
    def get_by_product_date(data_produto: datetime.date | None):
        if data_produto is None:
            data_produto = CargaSemanalDecomp.get_last_date()
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
        rows = __DB__.db_execute(query).rowcount
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
        rows = __DB__.db_execute(query).rowcount
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
        Busca os 2 períodos mais recentes (periodicidade_inicial),
        filtrando duplicatas por subsistema+dt_inicio e mantendo apenas registros com carga > 0.
        """
        today = datetime.date.today()

        # Step 1: Get the top 2 most recent periods that have already started
        query_periodos = db.select(
            CargaPmo.tb.c.periodicidade_inicial.distinct()
        ).order_by(
            CargaPmo.tb.c.periodicidade_inicial.desc()
        ).limit(3)

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
            raise HTTPException(
                status_code=404, detail=f"Dados de carga PMO para revisão {revisao} não encontrados")

        # Converte para DataFrame
        columns = ['id', 'carga', 'mes', 'revisao', 'subsistema',
                   'semana', 'dt_inicio', 'tipo', 'created_at', 'updated_at']
        df = pd.DataFrame(result, columns=columns)

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
    
class NewavePrevisoesCargas:
    tb: db.Table = __DB__.getSchema('newave_previsoes_cargas')

    @staticmethod
    def get_previsoes_cargas(data_revisao: Optional[datetime.date] = None, submercado: Optional[SubmercadosEnum] = None, patamar: Optional[PatamaresEnum] = None):
        query = db.select(
            NewavePrevisoesCargas.tb
        )
        if data_revisao:
            query = query.where(NewavePrevisoesCargas.tb.c.data_revisao == data_revisao)
        else:
            subq_max_dt = db.select(db.func.max(NewavePrevisoesCargas.tb.c.data_revisao)).scalar_subquery()
            query = query.where(NewavePrevisoesCargas.tb.c.data_revisao == subq_max_dt)   
            
        if submercado:
            query = query.where(NewavePrevisoesCargas.tb.c.submercado == submercado)
        if patamar:
            query = query.where(NewavePrevisoesCargas.tb.c.patamar == patamar)
        result = __DB__.db_execute(query).fetchall()
        
        df = pd.DataFrame(result)
        
        
        return df.to_dict('records')
    
    @staticmethod
    def delete_by_data_produto_data_revisao_equals(data_produto: datetime.date, data_revisao: datetime.date):
        query = db.delete(NewavePrevisoesCargas.tb).where(
            db.and_(
                NewavePrevisoesCargas.tb.c["data_produto"] == data_produto,
                NewavePrevisoesCargas.tb.c["data_revisao"] == data_revisao
            )
        )
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas deletadas da newave_previsoes_cargas")
        return None

    @staticmethod
    def create(body: List[NewavePrevisoesCargasCreateDto]):
        body_dict = [x.model_dump() for x in body]
        delete_dates = (body_dict[0]['data_produto'], body_dict[0]['data_revisao'])
        NewavePrevisoesCargas.delete_by_data_produto_data_revisao_equals(*delete_dates)
        
        query = db.insert(NewavePrevisoesCargas.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        logger.info(f"{rows} linhas adicionadas na newave_previsoes_cargas")
        return {"message": f"{rows} linhas adicionadas na newave_previsoes_cargas"}

class NewaveSistEnergia:
    tb: db.Table = __DB__.getSchema('tb_nw_sist_energia')

    @staticmethod
    def get_last_data_deck():
        query = db.select(
            db.func.max(NewaveSistEnergia.tb.c["dt_deck"])
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=['dt_deck'])
        return df.to_dict('records')

    @staticmethod
    def get_eol_by_last_data_deck_mes_ano_between(start: datetime.date, end: datetime.date):
        start = start.replace(day=1)
        end = end.replace(day=1)
        last_data_deck = NewaveSistEnergia.get_last_data_deck()[0]["dt_deck"]
        query = db.select(
            NewaveSistEnergia.tb.c["vl_geracao_eol"],
            NewaveSistEnergia.tb.c["cd_submercado"],
            NewaveSistEnergia.tb.c["vl_mes"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["dt_deck"]


        ).where(
            db.and_(
                db.cast(
                    db.func.concat(
                        NewaveSistEnergia.tb.c["vl_ano"],
                        '-',
                        db.func.lpad(NewaveSistEnergia.tb.c["vl_mes"], 2, '0'),
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
    def _delete_existing_by_dt_deck_versao(dt_deck_versao_combination: pd.DataFrame) -> int:
        
        combos = [(r.dt_deck, r.versao) for r in dt_deck_versao_combination.itertuples(index=False)]
            
        query = db.select(
            NewaveSistEnergia.tb.c.dt_deck, NewaveSistEnergia.tb.c.versao,
            db.func.min(NewaveSistEnergia.tb.c.created_at).label('created_min')
        ).where(
            db.tuple_(NewaveSistEnergia.tb.c.dt_deck, NewaveSistEnergia.tb.c.versao).in_(combos)
        ).group_by(NewaveSistEnergia.tb.c.dt_deck, NewaveSistEnergia.tb.c.versao)
        
        created_map = {(r.dt_deck, r.versao): r.created_min for r in __DB__.db_execute(query)}
        
        del_stmt = db.delete(NewaveSistEnergia.tb).where(db.tuple_(NewaveSistEnergia.tb.c.dt_deck, NewaveSistEnergia.tb.c.versao).in_(combos))
        rows_deleted = __DB__.db_execute(del_stmt).rowcount
        logger.info(f"{rows_deleted} registros deletados para {len(combos)} combinações")
        return created_map
    
    @staticmethod
    def post_newave_sist_energia(body: List[CargaNewaveSistemaEnergiaCreateDto]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
        if not body_dict:
            return {"message": "Nenhum registro para ser inserido encontrado."}
        
        df = pd.DataFrame(body_dict)
        dt_deck_versao_combination = df[['dt_deck', 'versao']].drop_duplicates()
        
        created_map = NewaveSistEnergia._delete_existing_by_dt_deck_versao(dt_deck_versao_combination)
        
        for r in body_dict:
            ca = created_map.get((r['dt_deck'], r['versao']))
            if ca is not None:
                r['created_at'] = ca
        
        query = db.insert(NewaveSistEnergia.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        
        return {"message": f"{rows} registros de sistema de energia Newave inseridos com sucesso"}
    
    @staticmethod
    def get_last_newave_sist_energia() -> List[dict]:
        max_dt_subq = db.select(db.func.max(NewaveSistEnergia.tb.c.dt_deck)).scalar_subquery()

        query = (
            db.select(
                NewaveSistEnergia.tb.c.cd_submercado,
                NewaveSistEnergia.tb.c.vl_ano,
                NewaveSistEnergia.tb.c.vl_mes,
                NewaveSistEnergia.tb.c.vl_energia_total,
                NewaveSistEnergia.tb.c.vl_geracao_pch,
                NewaveSistEnergia.tb.c.vl_geracao_pct,
                NewaveSistEnergia.tb.c.vl_geracao_eol,
                NewaveSistEnergia.tb.c.vl_geracao_ufv,
                NewaveSistEnergia.tb.c.vl_geracao_pch_mmgd,
                NewaveSistEnergia.tb.c.vl_geracao_pct_mmgd,
                NewaveSistEnergia.tb.c.vl_geracao_eol_mmgd,
                NewaveSistEnergia.tb.c.vl_geracao_ufv_mmgd,
                NewaveSistEnergia.tb.c.dt_deck,
                NewaveSistEnergia.tb.c.versao
            )
            .where(NewaveSistEnergia.tb.c.dt_deck == max_dt_subq)
        )

        result = __DB__.db_execute(query)
        rows = result.mappings().all()  

        return rows or []
       
    @staticmethod
    def get_sist_total_unsi_deck_values():
        """
        Obtém os valores de geração dos dois decks mais recentes, agrupados por deck,
        mês e ano, sem agrupamento por submercado.

        Returns:
            Lista com informações dos dois decks mais recentes, contendo dados
            de geração, organizados por mês e ano.
        """
        subquery = db.select(
            NewaveSistEnergia.tb.c["created_at"]
        ).distinct().order_by(
            NewaveSistEnergia.tb.c["created_at"]
        ).limit(2)

        query = db.select(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_mes"],
            NewaveSistEnergia.tb.c["vl_ano"],
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_pch"]).label("vl_geracao_pch"),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_pct"]).label("vl_geracao_pct"),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_eol"]).label("vl_geracao_eol"),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_ufv"]).label("vl_geracao_ufv")
        ).where(
            NewaveSistEnergia.tb.c["created_at"].in_(db.select(subquery.c.created_at))
        ).group_by(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NewaveSistEnergia.tb.c["dt_deck"].desc(),
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        )

        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'PCH', 'PCT', 'EOL', 'UFV'
        ])

        if df.empty:
            return []

        decks_data = []
        for (dt_deck, versao), deck_group in df.groupby(['dt_deck','versao']):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                'versao': versao,
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
            
        decks_data.reverse()

        return decks_data
    
    @staticmethod
    def get_sist_mmgd_total_deck_values():
        
        subquery = db.select(
            NewaveSistEnergia.tb.c["created_at"]
        ).distinct().order_by(
            NewaveSistEnergia.tb.c["created_at"]
        ).limit(2)

        query = db.select(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_mes"],
            NewaveSistEnergia.tb.c["vl_ano"],
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_pch_mmgd"]),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_pct_mmgd"]),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_eol_mmgd"]),
            db.func.sum(NewaveSistEnergia.tb.c["vl_geracao_ufv_mmgd"])
        ).where(
            NewaveSistEnergia.tb.c["created_at"].in_(db.select(subquery.c.created_at))
        ).group_by(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NewaveSistEnergia.tb.c["dt_deck"].desc(),
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        )

        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'PCH_MMGD', 'PCT_MMGD', 'EOL_MMGD', 'UFV_MMGD'
        ])
        
        if df.empty:
            return []

        decks_data = []
        for (dt_deck,versao), deck_group in df.groupby(['dt_deck','versao']):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "versao": versao,
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
            
        decks_data.reverse()
        
        return decks_data
    
    @staticmethod
    def put_sist_mmgd_total_deck_values(body: List[MMGDTotalUpdateDto]):
        """
        Atualiza os valores MMGD de geração na tabela tb_nw_sist_energia.
        Usa UPDATE individual para cada registro baseado na chave composta 
        (dt_deck, vl_mes, vl_ano, cd_submercado), preservando os outros campos.
        
        Args:
            body: Lista de objetos CargaNewaveSistemaEnergiaUpdateDto contendo os novos valores MMGD
            
        Returns:
            Dict com informações sobre a operação realizada
        """
        
        if not body:
            return {"message": "Nenhum dado fornecido para atualização"}
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
            elif isinstance(item['dt_deck'], datetime.datetime):
                item['dt_deck'] = item['dt_deck'].date()
        
        df_body_to_import = pd.DataFrame(body_dict)
        dt_deck = df_body_to_import['dt_deck'].iloc[0]
        
        updates_count = 0
        
        for _, row in df_body_to_import.iterrows():
            where_conditions = db.and_(
                NewaveSistEnergia.tb.c.dt_deck == row['dt_deck'],
                NewaveSistEnergia.tb.c.vl_ano == row['vl_ano'],
                NewaveSistEnergia.tb.c.vl_mes == row['vl_mes'],
                NewaveSistEnergia.tb.c.cd_submercado == row['cd_submercado']
            )
            
            update_values = {
                'vl_geracao_pch_mmgd': row['vl_geracao_pch_mmgd'],
                'vl_geracao_pct_mmgd': row['vl_geracao_pct_mmgd'],
                'vl_geracao_eol_mmgd': row['vl_geracao_eol_mmgd'],
                'vl_geracao_ufv_mmgd': row['vl_geracao_ufv_mmgd'],
                'versao': row['versao']
            }
            
            update_query = db.update(NewaveSistEnergia.tb).where(
                where_conditions
            ).values(update_values)
            
            result = __DB__.db_execute(update_query)
            rows_affected = result.rowcount
            
            if rows_affected > 0:
                updates_count += rows_affected
            
        
        return {
            "message": "Atualização MMGD concluída",
            "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
            "registros_atualizados": updates_count,
            "total_registros_processados": len(df_body_to_import)
        }
    
    @staticmethod
    def get_sist_total_carga_global_deck_values():
        
        # Get MMGD total values
        mmgd_base_values = NewaveCadic.get_cadic_total_mmgd_base_deck_values()
        boa_vista_values = NewaveCadic.get_cadic_boa_vista_values()
        
        subquery = db.select(
            NewaveSistEnergia.tb.c["created_at"]
        ).distinct().order_by(
            NewaveSistEnergia.tb.c["created_at"]
        ).limit(2)

        query = db.select(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_mes"],
            NewaveSistEnergia.tb.c["vl_ano"],
            db.func.sum(NewaveSistEnergia.tb.c["vl_energia_total"]).label("vl_energia_total")
        ).where(
            NewaveSistEnergia.tb.c["created_at"].in_(db.select(subquery.c.created_at))
        ).group_by(
            NewaveSistEnergia.tb.c["dt_deck"],
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        ).order_by(
            NewaveSistEnergia.tb.c["dt_deck"].desc(),
            NewaveSistEnergia.tb.c["versao"],
            NewaveSistEnergia.tb.c["vl_ano"],
            NewaveSistEnergia.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'vl_deck_energia_total'
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

        # Criando um dicionário para relacionar os valores de Boa Vista
        if boa_vista_values != None:
            boa_vista_dict = {}
            for deck in boa_vista_values:
                dt_deck = deck.get('dt_deck')
                boa_vista_dict[dt_deck] = {}

                for item in deck.get('data', []):
                    ano = item.get('vl_ano')
                    mes = item.get('vl_mes')

                    if ano not in boa_vista_dict[dt_deck]:
                        boa_vista_dict[dt_deck][ano] = {}

                    boa_vista_dict[dt_deck][ano][mes] = item.get('vl_boa_vista', 0)
        
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
        
        # Adicionar coluna de Boa Vista baseado no dicionário
        df['vl_deck_boa_vista'] = df.apply(
            lambda row: boa_vista_dict.get(
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
        
        # Calculando a carga global (soma da energia total, MMGD total e Boa Vista)
        df['vl_deck_carga_global'] = df['vl_deck_energia_total'] + df['vl_deck_mmgd_base'] + df['vl_deck_boa_vista']
        
        # Formatando o resultado final
        decks_data = []
        for (dt_deck,versao), deck_group in df.groupby(['dt_deck','versao']):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "versao": versao,
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
            
        decks_data.reverse()
        
        return decks_data
    
    @staticmethod
    def get_sist_total_carga_liquida_deck_values():
        """
        Calcula os valores de Carga Líquida (diferença entre Carga Global, MMGD Total, UNSI e ANDE)
        para os dois decks mais recentes.
        
        Returns:
            Lista com informações dos dois decks mais recentes, contendo dados
            de Carga Líquida, organizados por mês e ano.
        """
        # Obter dados de Carga Global, MMGD Total, UNSI e ANDE
        carga_global_values = NewaveSistEnergia.get_sist_total_carga_global_deck_values()
        mmgd_total_values = NewaveSistEnergia.get_sist_mmgd_total_deck_values()
        unsi_values = NewaveSistEnergia.get_sist_total_unsi_deck_values()
        ande_values = NewaveCadic.get_cadic_total_ande_deck_values()
            
        carga_liquida_values = []
        
        for deck_global in carga_global_values:
            dt_deck = deck_global.get('dt_deck')
            versao = deck_global.get('versao')
            
            # Encontrar os decks correspondentes para MMGD, UNSI e ANDE
            deck_mmgd = None
            deck_unsi = None
            deck_ande = None
            
            for deck in mmgd_total_values:
                if deck.get('dt_deck') == dt_deck:
                    deck_mmgd = deck
                    break
                    
            for deck in unsi_values:
                if deck.get('dt_deck') == dt_deck:
                    deck_unsi = deck
                    break
                    
            for deck in ande_values:
                if deck.get('dt_deck') == dt_deck:
                    deck_ande = deck
                    break
            
            if deck_mmgd and deck_unsi and deck_ande:
                # Criar dicionários para facilitar o acesso aos valores por (ano, mes)
                mmgd_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_deck_mmgd_total', 0) 
                           for item in deck_mmgd.get('data', [])}
                           
                unsi_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_deck_unsi', 0) 
                           for item in deck_unsi.get('data', [])}
                           
                ande_dict = {(item.get('vl_ano'), item.get('vl_mes')): item.get('vl_ande_total', 0) 
                           for item in deck_ande.get('data', [])}
                
                deck_liquida = {
                    "dt_deck": dt_deck,
                    "versao": versao,
                    "data": []
                }
                
                for item_global in deck_global.get('data', []):
                    ano = item_global.get('vl_ano')
                    mes = item_global.get('vl_mes')
                    valor_global = item_global.get('vl_deck_carga_global', 0)
                    
                    # Obter valores correspondentes de MMGD, UNSI e ANDE
                    valor_mmgd = mmgd_dict.get((ano, mes), 0)
                    valor_unsi = unsi_dict.get((ano, mes), 0)
                    valor_ande = ande_dict.get((ano, mes), 0)
                    
                    
                    # Calcular Carga Líquida = Carga Global - MMGD Total - UNSI + ANDE
                    carga_liquida = valor_global - valor_mmgd - valor_unsi + valor_ande
                    
                    item_liquida = {
                        "vl_ano": ano,
                        "vl_mes": mes,
                        "vl_deck_carga_liquida": int(round(carga_liquida))
                    }
                    
                    deck_liquida['data'].append(item_liquida)
                
                carga_liquida_values.append(deck_liquida)
                
        return carga_liquida_values
    
class NewaveCadic:
    tb:db.Table = __DB__.getSchema('tb_nw_cadic')
    
    @staticmethod
    def _delete_existing_by_dt_deck_versao(dt_deck_versao_combination: pd.DataFrame) -> int:
        
        combos = [(r.dt_deck, r.versao) for r in dt_deck_versao_combination.itertuples(index=False)]
            
        query = db.select(
            NewaveCadic.tb.c.dt_deck, NewaveCadic.tb.c.versao,
            db.func.min(NewaveCadic.tb.c.created_at).label('created_min')
        ).where(
            db.tuple_(NewaveCadic.tb.c.dt_deck, NewaveCadic.tb.c.versao).in_(combos)
        ).group_by(NewaveCadic.tb.c.dt_deck, NewaveCadic.tb.c.versao)
        
        created_map = {(r.dt_deck, r.versao): r.created_min for r in __DB__.db_execute(query)}
        
        del_stmt = db.delete(NewaveCadic.tb).where(db.tuple_(NewaveCadic.tb.c.dt_deck, NewaveCadic.tb.c.versao).in_(combos))
        rows_deleted = __DB__.db_execute(del_stmt).rowcount
        logger.info(f"{rows_deleted} registros deletados para {len(combos)} combinações")
        return created_map
    
    @staticmethod
    def post_newave_cadic(body: List[CargaNewaveCadicCreateDto]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
        if not body_dict:
            return {"message": "Nenhum registro para ser inserido encontrado."}
        
        df = pd.DataFrame(body_dict)
        dt_deck_versao_combination = df[['dt_deck', 'versao']].drop_duplicates()
        
        created_map = NewaveCadic._delete_existing_by_dt_deck_versao(dt_deck_versao_combination)
        
        for r in body_dict:
            ca = created_map.get((r['dt_deck'], r['versao']))
            if ca is not None:
                r['created_at'] = ca
        
        query = db.insert(NewaveCadic.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        
        return {"message": f"{rows} registros de sistema de energia Newave inseridos com sucesso"}
    
    @staticmethod
    def get_last_newave_cadic() -> List[CargaNewaveCadicReadDto]:
        
        max_dt_subq = db.select(db.func.max(NewaveCadic.tb.c.dt_deck)).scalar_subquery()
        
        query = db.select(
            NewaveCadic.tb.c.vl_ano,
            NewaveCadic.tb.c.vl_mes,
            NewaveCadic.tb.c.vl_const_itaipu,
            NewaveCadic.tb.c.vl_ande,
            NewaveCadic.tb.c.vl_mmgd_se,
            NewaveCadic.tb.c.vl_mmgd_s,
            NewaveCadic.tb.c.vl_mmgd_ne,
            NewaveCadic.tb.c.vl_mmgd_n,
            NewaveCadic.tb.c.vl_boa_vista,
            NewaveCadic.tb.c.dt_deck,
            NewaveCadic.tb.c.versao
        ).where(
            NewaveCadic.tb.c.dt_deck == max_dt_subq
        )
        
        result = __DB__.db_execute(query)
        rows = result.mappings().all()
        
        return rows or []
    
    @staticmethod
    def get_cadic_total_mmgd_base_deck_values():
        
        subquery = db.select(
            NewaveCadic.tb.c["created_at"]
        ).distinct().order_by(
            NewaveCadic.tb.c["created_at"]
        ).limit(2)

        query = db.select(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_mes"],
            NewaveCadic.tb.c["vl_ano"],
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_se"]).label("vl_mmgd_se"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_s"]).label("vl_mmgd_s"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_ne"]).label("vl_mmgd_ne"),
            db.func.sum(NewaveCadic.tb.c["vl_mmgd_n"]).label("vl_mmgd_n"),
        ).where(
            NewaveCadic.tb.c["created_at"].in_(db.select(subquery.c.created_at))
        ).group_by(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        ).order_by(
            NewaveCadic.tb.c["dt_deck"].desc(),
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'MMGD_SE', 'MMGD_S', 'MMGD_NE', 'MMGD_N'
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
        
        decks_data.reverse()

        return decks_data
    
    @staticmethod
    def get_cadic_boa_vista_values():
        query = db.select(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_mes"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_boa_vista"]
        ).order_by(
            NewaveCadic.tb.c["dt_deck"].desc(),
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'vl_boa_vista'
        ])
        
        if df.empty:
            return []

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
                    "vl_boa_vista": float(row['vl_boa_vista'])
                }
                deck_info["data"].append(data)

            decks_data.append(deck_info)

        decks_data.reverse()

        return decks_data
    
    @staticmethod
    def put_cadic_total_mmgd_base_deck_values(body: List[CargaNewaveCadicUpdateDto]):
        """
        Atualiza os valores MMGD base na tabela tb_nw_cadic.
        Usa UPDATE individual para cada registro baseado na chave composta 
        (dt_deck, vl_mes, vl_ano), preservando os outros campos.
        
        Args:
            body: Lista de objetos CargaNewaveCadicUpdateDto contendo os novos valores MMGD base
            
        Returns:
            Dict com informações sobre a operação realizada
        """
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
            elif isinstance(item['dt_deck'], datetime.datetime):
                item['dt_deck'] = item['dt_deck'].date()
        
        df_body_to_import = pd.DataFrame(body_dict)
        dt_deck = df_body_to_import['dt_deck'].iloc[0]
        
        updates_count = 0
        
        for _, row in df_body_to_import.iterrows():
            where_conditions = db.and_(
                NewaveCadic.tb.c.dt_deck == row['dt_deck'],
                NewaveCadic.tb.c.vl_ano == row['vl_ano'],
                NewaveCadic.tb.c.vl_mes == row['vl_mes']
            )
            
            update_values = {
                'vl_mmgd_se': row['vl_mmgd_se'],
                'vl_mmgd_s': row['vl_mmgd_s'],
                'vl_mmgd_ne': row['vl_mmgd_ne'],
                'vl_mmgd_n': row['vl_mmgd_n'],
                'versao': row['versao']
            }
            
            update_query = db.update(NewaveCadic.tb).where(
                where_conditions
            ).values(update_values)
            
            result = __DB__.db_execute(update_query)
            rows_affected = result.rowcount
            
            if rows_affected > 0:
                updates_count += rows_affected
            
        
        return {
            "message": "Atualização MMGD base concluída",
            "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
            "registros_atualizados": updates_count,
            "total_registros_processados": len(df_body_to_import)
        }
        
    @staticmethod
    def get_cadic_total_ande_deck_values():
        subquery = db.select(
            NewaveCadic.tb.c["created_at"]
        ).distinct().order_by(
            NewaveCadic.tb.c["created_at"]
        ).limit(2)

        query = db.select(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_mes"],
            NewaveCadic.tb.c["vl_ano"],
            db.func.sum(NewaveCadic.tb.c["vl_ande"]).label("vl_ande"),
            
        ).where(
            NewaveCadic.tb.c["created_at"].in_(db.select(subquery.c.created_at))
        ).group_by(
            NewaveCadic.tb.c["dt_deck"],
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        ).order_by(
            NewaveCadic.tb.c["dt_deck"].desc(),
            NewaveCadic.tb.c["versao"],
            NewaveCadic.tb.c["vl_ano"],
            NewaveCadic.tb.c["vl_mes"]
        )
        
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=[
            'dt_deck', 'versao', 'vl_mes', 'vl_ano', 'vl_ande'
        ])
        
        if df.empty:
            return []

        decks_data = []
        for (dt_deck,versao), deck_group in df.groupby(['dt_deck','versao']):
            deck_info = {
                "dt_deck": dt_deck.strftime('%Y-%m-%d') if isinstance(dt_deck, datetime.date) else str(dt_deck),
                "versao": versao,
                "data": []
            }

            for (ano, mes), month_group in deck_group.groupby(['vl_ano', 'vl_mes']):
                row = month_group.iloc[0]

                total_geracao = float(row['vl_ande'])
            
                data = {
                    "vl_mes": int(mes),
                    "vl_ano": int(ano),
                    "vl_ande_total": total_geracao
                }

                deck_info["data"].append(data)

            decks_data.append(deck_info)
            
        decks_data.reverse()

        return decks_data
    
class NewavePatamarCargaUsina:
    tb: db.Table = __DB__.getSchema('newave_patamar_carga_usina')
    
    @staticmethod
    def _delete_existing_by_dt_deck_versao(dt_deck_versao_combination: pd.DataFrame) -> int:
        
        combos = [(r.dt_deck, r.versao) for r in dt_deck_versao_combination.itertuples(index=False)]
            
        query = db.select(
            NewavePatamarCargaUsina.tb.c.dt_deck, NewavePatamarCargaUsina.tb.c.versao,
            db.func.min(NewavePatamarCargaUsina.tb.c.created_at).label('created_min')
        ).where(
            db.tuple_(NewavePatamarCargaUsina.tb.c.dt_deck, NewavePatamarCargaUsina.tb.c.versao).in_(combos)
        ).group_by(NewavePatamarCargaUsina.tb.c.dt_deck, NewavePatamarCargaUsina.tb.c.versao)
        
        created_map = {(r.dt_deck, r.versao): r.created_min for r in __DB__.db_execute(query)}
        
        del_stmt = db.delete(NewavePatamarCargaUsina.tb).where(db.tuple_(NewavePatamarCargaUsina.tb.c.dt_deck, NewavePatamarCargaUsina.tb.c.versao).in_(combos))
        rows_deleted = __DB__.db_execute(del_stmt).rowcount
        logger.info(f"{rows_deleted} registros deletados para {len(combos)} combinações")
        return created_map

    @staticmethod
    def post_newave_patamar_carga_usina(body: List[NewavePatamarCargaUsinaSchema]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
        if not body_dict:
            return {"message": "Nenhum registro para ser inserido encontrado."}
        
        df = pd.DataFrame(body_dict)
        dt_deck_versao_combination = df[['dt_deck', 'versao']].drop_duplicates()
        
        created_map = NewavePatamarCargaUsina._delete_existing_by_dt_deck_versao(dt_deck_versao_combination)
        
        for r in body_dict:
            ca = created_map.get((r['dt_deck'], r['versao']))
            if ca is not None:
                r['created_at'] = ca
        
        query = db.insert(NewavePatamarCargaUsina.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        
        return {"message": f"{rows} registros de Patamar de Carga inseridos com sucesso"}
  
    @staticmethod
    def get_patamar_carga_by_dt_deck(dt_deck: datetime.date):
        query = db.select(NewavePatamarCargaUsina.tb).where(
            NewavePatamarCargaUsina.tb.c.dt_deck == dt_deck
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            raise HTTPException(
                status_code=404, detail=f"Patamar de intercâmbio para a data {dt_deck} não encontrado"
            )

        df = pd.DataFrame(result, columns=NewavePatamarCargaUsina.tb.columns.keys())
        return df.to_dict('records')
  
    @staticmethod
    def get_patamar_carga_by_dt_referente(dt_inicial: datetime.date, dt_final: datetime.date, indice_bloco: Optional[IndiceBlocoEnum] = None):
        """
        Obtém os patamares de carga de usina para um intervalo de datas e, opcionalmente, um índice de bloco específico.
        
        Args:
            dt_inicial: Data inicial do intervalo
            dt_final: Data final do intervalo
            indice_bloco: Índice de bloco opcional para filtrar os resultados (PCH, PCT, EOL, UFV, PCH_MMGD, PCT_MMGD, EOL_MMGD, UFV_MMGD)
        
        Returns:
            Lista de patamares de carga como dicionários
        """
        conditions = [
            NewavePatamarCargaUsina.tb.c.dt_referente >= dt_inicial,
            NewavePatamarCargaUsina.tb.c.dt_referente <= dt_final
        ]
        
        if indice_bloco is not None:
            conditions.append(NewavePatamarCargaUsina.tb.c.indice_bloco == indice_bloco.value)
        
        query = db.select(NewavePatamarCargaUsina.tb).where(
            db.and_(*conditions)
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            if indice_bloco is not None:
                detail_msg = f"Patamar de carga de usina não encontrado para o intervalo {dt_inicial} a {dt_final} e índice de bloco {indice_bloco.value}"
            else:
                detail_msg = f"Patamar de carga de usina não encontrado para o intervalo {dt_inicial} a {dt_final}"
            
            raise HTTPException(status_code=404, detail=detail_msg)

        df = pd.DataFrame(result, columns=NewavePatamarCargaUsina.tb.columns.keys())
        return df.to_dict('records')
    
class NewavePatamarIntercambio:
    tb: db.Table = __DB__.getSchema('newave_patamar_intercambio')
    
    @staticmethod
    def _delete_existing_by_dt_deck_versao(dt_deck_versao_combination: pd.DataFrame) -> int:
        
        combos = [(r.dt_deck, r.versao) for r in dt_deck_versao_combination.itertuples(index=False)]
            
        query = db.select(
            NewavePatamarIntercambio.tb.c.dt_deck, NewavePatamarIntercambio.tb.c.versao,
            db.func.min(NewavePatamarIntercambio.tb.c.created_at).label('created_min')
        ).where(
            db.tuple_(NewavePatamarIntercambio.tb.c.dt_deck, NewavePatamarIntercambio.tb.c.versao).in_(combos)
        ).group_by(NewavePatamarIntercambio.tb.c.dt_deck, NewavePatamarIntercambio.tb.c.versao)
        
        created_map = {(r.dt_deck, r.versao): r.created_min for r in __DB__.db_execute(query)}
        
        del_stmt = db.delete(NewavePatamarIntercambio.tb).where(db.tuple_(NewavePatamarIntercambio.tb.c.dt_deck, NewavePatamarIntercambio.tb.c.versao).in_(combos))
        rows_deleted = __DB__.db_execute(del_stmt).rowcount
        logger.info(f"{rows_deleted} registros deletados para {len(combos)} combinações")
        return created_map

    @staticmethod
    def post_newave_patamar_intercambio(body: List[NewavePatamarIntercambioSchema]):
        
        body_dict = [x.model_dump() for x in body]
        
        for item in body_dict:
            if isinstance(item['dt_deck'], str):
                item['dt_deck'] = datetime.datetime.strptime(item['dt_deck'], '%Y-%m-%d').date()
        if not body_dict:
            return {"message": "Nenhum registro para ser inserido encontrado."}
        
        df = pd.DataFrame(body_dict)
        dt_deck_versao_combination = df[['dt_deck', 'versao']].drop_duplicates()
        
        created_map = NewavePatamarIntercambio._delete_existing_by_dt_deck_versao(dt_deck_versao_combination)
        
        for r in body_dict:
            ca = created_map.get((r['dt_deck'], r['versao']))
            if ca is not None:
                r['created_at'] = ca
        
        query = db.insert(NewavePatamarIntercambio.tb).values(body_dict)
        rows = __DB__.db_execute(query).rowcount
        
        return {"message": f"{rows} registros de Patamar de Intercambio inseridos com sucesso"}
    
    @staticmethod
    def get_patamar_intercambio_by_dt_deck(dt_deck: datetime.date):
        query = db.select(NewavePatamarIntercambio.tb).where(
            NewavePatamarIntercambio.tb.c.dt_deck == dt_deck
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            raise HTTPException(
                status_code=404, detail=f"Patamar de intercâmbio para a data {dt_deck} não encontrado"
            )

        df = pd.DataFrame(result, columns=NewavePatamarIntercambio.tb.columns.keys())
        return df.to_dict('records')

    @staticmethod
    def get_patamar_intercambio_by_dt_referente(dt_inicial: datetime.date, dt_final: datetime.date):
        """
        Obtém os patamares de intercâmbio para um intervalo de datas de referência.
        
        Args:
            dt_inicial: Data inicial do intervalo
            dt_final: Data final do intervalo
        
        Returns:
            Lista de patamares de intercâmbio como dicionários
        """
        conditions = [
            NewavePatamarIntercambio.tb.c.dt_referente >= dt_inicial,
            NewavePatamarIntercambio.tb.c.dt_referente <= dt_final
        ]
        
        query = db.select(NewavePatamarIntercambio.tb).where(
            db.and_(*conditions)
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            detail_msg = f"Patamar de intercâmbio não encontrado para o intervalo {dt_inicial} a {dt_final}"
            raise HTTPException(status_code=404, detail=detail_msg)

        df = pd.DataFrame(result, columns=NewavePatamarIntercambio.tb.columns.keys())
        return df.to_dict('records')

class CheckCvu:
    tb: db.Table = __DB__.getSchema('check_cvu')

    @staticmethod
    def create(body: CheckCvuCreateDto) -> CheckCvuReadDto:
        create_data = body.model_dump(exclude_none=True)

        query = db.insert(CheckCvu.tb).values(**create_data)
        result = __DB__.db_execute(query)

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

        result = __DB__.db_execute(update_query)

        if result.rowcount == 0:
            raise Exception("Erro ao atualizar status do check cvu")

        logger.info(
            f"Status do check cvu ID {check_cvu_id} atualizado para: {status}")
        return CheckCvu.get_by_id(check_cvu_id)

    @staticmethod
    def get_by_data_atualizacao_tipo_cvu(data_atualizacao: datetime.datetime, tipo_cvu: TipoCvuEnum) -> CheckCvuReadDto:
        select_query = db.select(
            CheckCvu.tb
        ).where(db.and_(
                CheckCvu.tb.c['tipo_cvu'] == tipo_cvu.value,
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

        select_query = db.select(CheckCvu.tb).order_by(CheckCvu.tb.c.id.desc()).limit(page_size).offset(offset)
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

class DessemPrevisao:
    tb_ds_carga: db.Table = __DB__.getSchema('tb_ds_carga')
    tb_submercado: db.Table = __DB__.getSchema('tb_submercado')

    @staticmethod
    def get_previsao_dessem():
        """
        Pega previsão IPDO:
        - deck mais recente completo
        - se faltarem registros de hoje, busca somente hoje no deck anterior
        Agrega por dia e sigla, e retorna dict aninhado.
        """
        hoje = datetime.date.today()

        max_deck_subquery = db.select(
            db.func.max(DessemPrevisao.tb_ds_carga.c.id_deck)
        ).scalar_subquery()

        prev_deck_subquery = db.select(
            db.func.max(DessemPrevisao.tb_ds_carga.c.id_deck)
        ).where(
            DessemPrevisao.tb_ds_carga.c.id_deck < max_deck_subquery
        ).scalar_subquery()

        query_max = db.select(
            DessemPrevisao.tb_submercado.c.str_sigla.label('sigla'),
            DessemPrevisao.tb_ds_carga.c.dataHora,
            DessemPrevisao.tb_ds_carga.c.vl_carga
        ).select_from(
            DessemPrevisao.tb_ds_carga.join(
                DessemPrevisao.tb_submercado,
                DessemPrevisao.tb_ds_carga.c.cd_submercado == DessemPrevisao.tb_submercado.c.cd_submercado
            )
        ).where(
            DessemPrevisao.tb_ds_carga.c.id_deck == max_deck_subquery
        )

        rows_max = __DB__.db_execute(query_max).fetchall()
        df_max = pd.DataFrame(rows_max, columns=['sigla', 'dataHora', 'vl_carga'])
        
        if df_max.empty:
            return {}

        df_max['day'] = pd.to_datetime(df_max['dataHora']).dt.date.astype(str)

        existe_hoje = (df_max['day'] == str(hoje)).any()

        if not existe_hoje:
            query_prev = db.select(
                DessemPrevisao.tb_submercado.c.str_sigla.label('sigla'),
                DessemPrevisao.tb_ds_carga.c.dataHora,
                DessemPrevisao.tb_ds_carga.c.vl_carga
            ).select_from(
                DessemPrevisao.tb_ds_carga.join(
                    DessemPrevisao.tb_submercado,
                    DessemPrevisao.tb_ds_carga.c.cd_submercado == DessemPrevisao.tb_submercado.c.cd_submercado
                )
            ).where(
                db.and_(
                    DessemPrevisao.tb_ds_carga.c.id_deck == prev_deck_subquery,
                    db.func.date(DessemPrevisao.tb_ds_carga.c.dataHora) == hoje
                )
            )

            rows_prev = __DB__.db_execute(query_prev).fetchall()
            df_prev = pd.DataFrame(rows_prev, columns=['sigla', 'dataHora', 'vl_carga'])
            
            if not df_prev.empty:
                df_prev['day'] = pd.to_datetime(df_prev['dataHora']).dt.date.astype(str)
                df_max = pd.concat([df_max, df_prev], ignore_index=True)

        daily_avg = (
            df_max
            .groupby(['sigla', 'day'], as_index=False)
            .agg(avg_vl_carga=('vl_carga', 'mean'))
        )

        nested = {}
        for sigla, group in daily_avg.groupby('sigla'):
            nested[sigla] = group[['day', 'avg_vl_carga']].to_dict('records')

        return nested

class RestricoesEletricas:
    tb: db.Table = __DB__.getSchema('restricao_eletrica')

    @staticmethod
    def remove_restricoes_eletricas_by_data_produto(data_produto: datetime.date):
        query = db.delete(RestricoesEletricas.tb).where(
            RestricoesEletricas.tb.c['data_produto'] == data_produto
        )
        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} restricoes eletricas removidas para a data {data_produto}")
        return None


    @staticmethod
    def post_restricoes_eletricas(body: List[RestricoesEletricasSchema]):
        data = [x.model_dump() for x in body]
        RestricoesEletricas.remove_restricoes_eletricas_by_data_produto(data[0]['data_produto'])
        query = db.insert(RestricoesEletricas.tb).values(data)
        result = __DB__.db_execute(query)
        
        if result.rowcount == 0:
            raise HTTPException(status_code=500, detail="Erro ao inserir restricao eletrica")
        
        return {"message": "Restricao eletrica inserida com sucesso"}


    @staticmethod
    def get_last_data_produto():
        query = db.select(
            db.func.max(RestricoesEletricas.tb.c['data_produto'])
        )
        result = __DB__.db_execute(query).scalar()
        
        if not result:
            raise HTTPException(status_code=404, detail="Nenhuma data de produto encontrada")
        
        return result


    @staticmethod
    def get_restricoes_eletricas_by_data_produto(
        data_produto: Optional[datetime.date] = None
    ):
        if not data_produto:
            data_produto = RestricoesEletricas.get_last_data_produto()
            
        query = db.select(RestricoesEletricas.tb).where(
            RestricoesEletricas.tb.c['data_produto'] == data_produto
        )
        
        result = __DB__.db_execute(query).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="Nenhuma restrição elétrica encontrada")
        
        df = pd.DataFrame(result, columns=RestricoesEletricas.tb.columns.keys())
        return df.to_dict('records')
    
    @staticmethod
    def get_datas_produto():
        query = db.select(
            db.func.distinct(RestricoesEletricas.tb.c['data_produto'])
        ).order_by(
            RestricoesEletricas.tb.c['data_produto'].desc()
        )
        
        result = __DB__.db_execute(query).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="Nenhuma data de produto encontrada")
        
        datas_produto = [row[0] for row in result]
        return datas_produto

class DadosHidraulicosUhe:
    tb: db.Table = __DB__.getSchema('dados_hidraulicos_uhe')
    
    
    @staticmethod
    def delete_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date
    ):
        if not data_fim:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.delete(DadosHidraulicosUhe.tb).where(
            query_param
        )
        

        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} dados hidraulicos UHE removidos entre {data_inicio} e {data_fim}")
        return None
    
    @staticmethod
    def create(body: List[DadosHidraulicosUheCreateDto]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)
        data_max = df['data_referente'].max()
        data_min = df['data_referente'].min()
        df_dados_atuais = pd.DataFrame(DadosHidraulicosUhe.get_by_data_referente_entre(data_min, data_max))
        if not df_dados_atuais.empty:
            df = df.combine_first(df_dados_atuais)
        DadosHidraulicosUhe.delete_by_data_referente_entre(data_min, data_max)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        query = db.insert(DadosHidraulicosUhe.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query).rowcount
        logger.info(f"{result} dados hidraulicos UHE inseridos entre {data_min} e {data_max}")
        return body_dict

    
    @staticmethod
    def get_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date | None = None
    ):
        
        if not data_fim:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosUhe.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.select(DadosHidraulicosUhe.tb).where(
            query_param
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            return [{}]
            raise HTTPException(
                status_code=404, detail=f"Dados hidraulicos UHE entre {data_inicio} e {data_fim} nao encontrados"
            )

        df = pd.DataFrame(result, columns=DadosHidraulicosUhe.tb.columns.keys())
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
class DadosHidraulicosSubsistema:
    tb: db.Table = __DB__.getSchema('dados_hidraulicos_subsistema')
    
    @staticmethod
    def delete_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date
    ):
        query = db.delete(DadosHidraulicosSubsistema.tb).where(
            DadosHidraulicosSubsistema.tb.c.data_referente.between(data_inicio, data_fim)
        )
        result = __DB__.db_execute(query)
        logger.info(f"{result.rowcount} dados hidraulicos subsistema removidos entre {data_inicio} e {data_fim}")
        return None
    
    @staticmethod
    def create(body: List[DadosHidraulicosSubsistemaCreateDto]):
        body_dict = [x.model_dump() for x in body]
        df = pd.DataFrame(body_dict)
        data_max = df['data_referente'].max()
        data_min = df['data_referente'].min()
        df_dados_atuais = pd.DataFrame(DadosHidraulicosSubsistema.get_by_data_referente_entre(data_min, data_max))
        if not df_dados_atuais.empty:
            df = df.combine_first(df_dados_atuais)
        DadosHidraulicosSubsistema.delete_by_data_referente_entre(data_min, data_max)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        query = db.insert(DadosHidraulicosSubsistema.tb).values(df.to_dict('records'))
        result = __DB__.db_execute(query).rowcount
        logger.info(f"{result} dados hidraulicos subsistema inseridos entre {data_min} e {data_max}")
        return body_dict

    
    @staticmethod
    def get_by_data_referente_entre(
        data_inicio: datetime.date,
        data_fim: datetime.date | None = None
    ):
        if not data_fim:
            query_param = (DadosHidraulicosSubsistema.tb.c.data_referente >= data_inicio)
        else:
            query_param = (DadosHidraulicosSubsistema.tb.c.data_referente.between(data_inicio, data_fim))
        query = db.select(DadosHidraulicosSubsistema.tb).where(
            query_param
        )

        result = __DB__.db_execute(query).fetchall()

        if not result:
            return [{}]
        df = pd.DataFrame(result, columns=DadosHidraulicosSubsistema.tb.columns.keys())
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
