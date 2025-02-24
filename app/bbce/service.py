from sys import path
import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
from typing import List, Optional
import datetime

from app.schemas import ProdutoInteresseSchema, CategoriaNegociacaoEnum

from app.core.database.wx_dbClass import db_mysql_master

__DB__ = db_mysql_master('bbce')
prod = True

class Produto:
    tb:db.Table = __DB__.getSchema('tb_produtos')
    
    @staticmethod
    def get_ids_produtos_by_str_produtos(str_produtos:List[str]):
        __DB__.connect()
        query = db.select(
            Produto.tb.c["id_produto"],
            Produto.tb.c["str_produto"]
        ).where(
            Produto.tb.c["str_produto"].in_(str_produtos)
        )
        result = __DB__.db_execute(query, commit=prod)
        df = pd.DataFrame(result, columns=["id_produto", "str_produto"])
        return df.to_dict("records")

class ProdutoInteresse:
    tb:db.Table = __DB__.getSchema('tb_produtos_interesse')
    
    @staticmethod
    def get_all():
        __DB__.connect()
        select = db.select(
            ProdutoInteresse.tb.c['id_produto'],
            ProdutoInteresse.tb.c['ordem'],
            Produto.tb.c['str_produto']
        ).join(
            Produto.tb, Produto.tb.c['id_produto'] == ProdutoInteresse.tb.c['id_produto']
        )
        result = __DB__.db_execute(select, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=["id", "ordem", "str_produto"])
        return df.to_dict("records")
    
    @staticmethod
    def post(produtos_interesse: List[ProdutoInteresseSchema]):
        __DB__.connect()
        records:List[dict] = [obj.model_dump() for obj in produtos_interesse]
        df_produtos_ordem = pd.DataFrame(records)
        df_produtos_interesse = pd.DataFrame(Produto.get_ids_produtos_by_str_produtos([prod["str_produto"] for prod in records]))
        
        df = df_produtos_interesse.merge(df_produtos_ordem)[["id_produto", "ordem"]]
        
        __DB__.db_execute(ProdutoInteresse.tb.delete(), commit=prod)
        
        __DB__.db_execute(ProdutoInteresse.tb.insert().values(df.to_dict("records")), commit=prod)
        return df.to_dict("records")
    
class Negociacoes:
    tb:db.Table = __DB__.getSchema('tb_negociacoes')
    
    @staticmethod
    def get_datahora_ultima_negociacao(categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None):
        __DB__.connect()
        query = db.select(
          db.func.max(
            Negociacoes.tb.c['dt_criacao']
          )
        )
        if categoria_negociacao:
                query = query.join(
                CategoriaNegociacao.tb, CategoriaNegociacao.tb.c["id"] == Negociacoes.tb.c["id_categoria_negociacao"]
            ).where(
                CategoriaNegociacao.tb.c["nome"] == categoria_negociacao.value
            )
            
            
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['ultimo_update'])
        df['ultimo_update'] = df['ultimo_update'].dt.strftime('%Y-%m-%d %H:%M')
        return df.to_dict('records')[0]




class NegociacoesResumo:
    tb:db.Table = __DB__.getSchema('tb_negociacoes_resumo')
    @staticmethod
    def get_negociacao_bbce(produto:str, categoria_negociacao:Optional[CategoriaNegociacaoEnum]):
        __DB__.connect()
        query = db.select(
            NegociacoesResumo.tb.c['data'],
            NegociacoesResumo.tb.c['preco_abertura'],
            NegociacoesResumo.tb.c['preco_maximo'],
            NegociacoesResumo.tb.c['preco_minimo'],
            NegociacoesResumo.tb.c['preco_fechamento'],
            NegociacoesResumo.tb.c['volume'],
            NegociacoesResumo.tb.c['preco_medio']
            )
        if categoria_negociacao:
            query = query.join(
                Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).join(
                    CategoriaNegociacao.tb, CategoriaNegociacao.tb.c["id"] == NegociacoesResumo.tb.c["id_categoria_negociacao"]
                    ).where(db.and_(
                        Produto.tb.c['str_produto'] == produto,
                        CategoriaNegociacao.tb.c["nome"] == categoria_negociacao.value
                    ))
        else:
            query = query.join(
                Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).where(
                    Produto.tb.c['str_produto'] == produto,
                    )
        print(query)
        result = __DB__.db_execute(query, commit=prod).fetchall()
        df = pd.DataFrame(result, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'preco_medio'])
        df['date'] = df['date'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        return df.to_dict('records')

    @staticmethod
    def spread_preco_medio_negociacoes(produto1:str, produto2:str, categoria_negociacao:Optional[CategoriaNegociacaoEnum]):
        __DB__.connect()
        resposta1 = NegociacoesResumo.get_negociacao_bbce(produto1, categoria_negociacao)
        resposta2 = NegociacoesResumo.get_negociacao_bbce(produto2, categoria_negociacao)

        df1 = pd.DataFrame(resposta1)
        df2 = pd.DataFrame(resposta2)
        df1['date'] = pd.to_datetime(df1['date'])
        df2['date'] = pd.to_datetime(df2['date'])


        dt_fim = max(df1['date'].max(), df2['date'].max())
        dt_inicio = min(df1['date'].min(), df2['date'].min())

        df1 = df1.set_index('date')
        df2 = df2.set_index('date')

        index_datas = pd.date_range(start=dt_inicio, end=dt_fim)

        df_combinado = pd.DataFrame(index=index_datas)
        df_combinado['preco_medio_1'] = df1['preco_medio']
        df_combinado['preco_medio_2'] = df2['preco_medio']
        df_combinado.ffill(inplace=True) 
        df_combinado.bfill(inplace=True) 
        df_combinado['spread'] = df_combinado['preco_medio_2']-df_combinado['preco_medio_1']
        df_combinado    = df_combinado.reset_index()

        df_spread = df_combinado[['index','spread']]
        df_spread.loc[:,'index'] = df_spread['index'].dt.strftime('%Y-%m-%d')
        df_spread.dropna(inplace=True)
        df_spread = df_spread.rename({'index':'date'},axis=1)
        
        response = df_spread[['date', 'spread']].to_dict(orient='records')
        return response

    @staticmethod
    def get_negociacoes_fechamento_interesse_por_data(data:datetime.date, categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None):
        __DB__.connect()
        df_produtos_interesse = pd.DataFrame(ProdutoInteresse.get_all())[["str_produto", "id"]]
        df_produtos_interesse = df_produtos_interesse.rename(columns={'str_produto':'produto'}) 
                
        cte =  db.select(
                Produto.tb.c['str_produto'],
                NegociacoesResumo.tb.c['id_produto'],
                db.func.timestamp(NegociacoesResumo.tb.c['data'], NegociacoesResumo.tb.c['hora_fechamento']),
                NegociacoesResumo.tb.c['preco_fechamento'],
                db.func.row_number().over(
                        partition_by=[NegociacoesResumo.tb.c['id_produto']],
                        order_by=db.desc(NegociacoesResumo.tb.c['data'])
                ).label('row_num')
        )
        
        if categoria_negociacao:
            cte = (
                cte.join(
                Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).join(
                    CategoriaNegociacao.tb, CategoriaNegociacao.tb.c["id"] == NegociacoesResumo.tb.c["id_categoria_negociacao"]
                    ).where(db.and_(
                CategoriaNegociacao.tb.c["nome"] == categoria_negociacao.value,
                NegociacoesResumo.tb.c['data'] <= data,
                Produto.tb.c['str_produto'].in_(df_produtos_interesse['produto'].tolist())
                )).cte('cte_groups')
            )
        else:
            cte = (
                cte.join(
                Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).where(db.and_(
                NegociacoesResumo.tb.c['data'] <= data,
                Produto.tb.c['str_produto'].in_(df_produtos_interesse['produto'].tolist())
                )).cte('cte_groups')
            )
        
        query = (
                db.select(cte)
                .where(cte.c['row_num'] == 1)
                .order_by(cte.c['id_produto'])
        )
        
        result = __DB__.db_execute(query, commit=prod)
        df = pd.DataFrame(result, columns=['produto','id_produto','datetime_fechamento','preco_fechamento', 'rn'])
        df['datetime_fechamento'] = pd.to_datetime(df['datetime_fechamento']).dt.strftime('%Y-%m-%d %H:%M')
        
        df = df.round(2)
        
        df = pd.merge(df_produtos_interesse, df, on='produto', how='left')
        df['produto'] = df['produto'].str.extract(r'SE CON (.{1,}) - Preço Fixo')
        df = df[['produto', 'preco_fechamento', 'datetime_fechamento']]
        df = df.replace({np.nan: None})
        
        return df.to_dict('records')

    @staticmethod
    def get_negociacoes_interesse_por_data(data:datetime.date, categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None):
        __DB__.connect()
        df_produtos_interesse = pd.DataFrame(ProdutoInteresse.get_all())
        df_produtos_interesse = df_produtos_interesse.rename(columns={'str_produto':'produto'})
        
        query = db.select(
            Produto.tb.c['str_produto'],
            NegociacoesResumo.tb.c['data'],
            NegociacoesResumo.tb.c['preco_abertura'],
            NegociacoesResumo.tb.c['preco_maximo'], 
            NegociacoesResumo.tb.c['preco_minimo'],
            NegociacoesResumo.tb.c['preco_fechamento'],
            NegociacoesResumo.tb.c['volume'],
            NegociacoesResumo.tb.c['preco_medio'],
            NegociacoesResumo.tb.c['total_negociacoes'],
            NegociacoesResumo.tb.c['hora_fechamento'],
        )
        if categoria_negociacao:
                query = query.join(
                    Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).join(
                CategoriaNegociacao.tb, CategoriaNegociacao.tb.c["id"] == NegociacoesResumo.tb.c["id_categoria_negociacao"]
            ).where(db.and_(
                NegociacoesResumo.tb.c['data'] == data,
                Produto.tb.c['str_produto'].in_(df_produtos_interesse['produto'].tolist()),
                CategoriaNegociacao.tb.c["nome"] == categoria_negociacao.value
            ))
        else:
            query = query.join(
                    Produto.tb, Produto.tb.c['id_produto'] == NegociacoesResumo.tb.c['id_produto']
                ).where(db.and_(
                NegociacoesResumo.tb.c['data'] == data,
                Produto.tb.c['str_produto'].in_(df_produtos_interesse['produto'].tolist())
                ))
        result = __DB__.db_execute(query, commit=prod).all()
        df = pd.DataFrame(result, columns=['produto', 'date', 'open', 'high', 'low', 'close', 'volume', 'preco_medio', 'total', 'hora_fechamento'])
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        df_fechamento_anterior = pd.DataFrame(NegociacoesResumo.get_negociacoes_fechamento_interesse_por_data(data-datetime.timedelta(days=1), categoria_negociacao))
        df_fechamento_anterior.columns = ['produto', 'preco_fechamento_anterior', 'datetime_fechamento_anterior']
        

        df = pd.merge(df_produtos_interesse, df, on='produto', how='left')
        df['produto'] = df['produto'].str.extract(r'SE CON (.{1,}) - Preço Fixo')
        df = pd.merge(df_fechamento_anterior, df, on='produto', how='left')
        
        df['change_percent'] = ((df['close'] / df['preco_fechamento_anterior']) * 100) - 100
        df['change_value'] = df['close'] - df['preco_fechamento_anterior']
        
        # df = df.fillna(None)
        df = df.sort_values("ordem")
        
        df = df.round(2)
        df = df.replace({np.nan: None})
        return df.to_dict('records')

    @staticmethod
    def get_html_table_negociacoes_bbce(data:datetime.date, categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None):
        __DB__.connect()
        style = '''
<style>
.verde {
    color: #06720f !important;
    border-color: #000000;   
}
.vermelho {
    color: #FF0513 !important;
    border-color: #000000;   
    
}
body {
    font-family: sans-serif;
}
th,
td {
    padding: 8px 4px;
    text-align: center;
    border: 0.5px solid;
}

table {
    border-collapse: collapse;
}
.MEN_IMPAR {
background-color: #C7DBD2;
}
.MEN_PAR {
  background-color: #D1E7DD;
}

.TRI_IMPAR {
  background-color: #F2E7C3;
}
.TRI_PAR {
  background-color: #FFF3CD;
}

.SEM_IMPAR {
  background-color: #ECCCCF;
}
.SEM_PAR {
  background-color: #F8D7DA;
}

.ANU_IMPAR {
  background-color: #C5D7F2;
}
.ANU_PAR {
  background-color: #CFE2FF;
}
.dark{
  background-color: #212529;
  color: #FFFFFF;
}
*{
  font-weight: bold!important;
  }
</style>
        '''
        html = ""
        if not categoria_negociacao:
            categorias = [CategoriaNegociacaoEnum("Mesa"), CategoriaNegociacaoEnum("Boleta Eletronica")]
        else: 
            categorias = [categoria_negociacao]
        for categoria in categorias:
            df = pd.DataFrame(NegociacoesResumo.get_negociacoes_interesse_por_data(data,categoria))
            data_hora_ultima_negociacao = Negociacoes.get_datahora_ultima_negociacao(categoria)
            df["datetime_fechamento_anterior"] = pd.to_datetime(df['datetime_fechamento_anterior']).dt.strftime("%d/%m")
            df = df.fillna("-")

            df["datetime_fechamento_anterior"]

            df["hora_fechamento"] = df["hora_fechamento"].astype(str).str[:-3]
            df[['granularidade', 'produto']] = df['produto'].str.split(' ', n=1, expand=True)
            df = df[["granularidade","produto" , "preco_fechamento_anterior", "datetime_fechamento_anterior", "close","hora_fechamento" , "volume", "total", "change_value", "change_percent", "open", "high", "low"]]

            html += f'''
                <div>
                <h3>Categoria: {categoria.value}</h3>
                <h3>Última atualização: {datetime.datetime.strptime(data_hora_ultima_negociacao["ultimo_update"], "%Y-%m-%d %H:%M").strftime("%d/%m/%Y %H:%M")}</h3>
                </div>
                
            <table><thead class="dark"><tr><th scope="col" colspan="2">Nome</th><th scope="col" colspan="2">Fechamento Anterior</th><th scope="col" colspan="2">Ult. Negoc</th><th scope="col">Volume(MWm)</th><th scope="col">Total</th><th scope="col">Variação</th><th scope="col">Variação(%)</th><th scope="col">Abertura</th><th scope="col">Máxima</th><th scope="col">Mínima</th></tr></thead><tbody id="table-body">'''
            for i, row in df.iterrows():
                impar_ou_par = "_PAR" if i%2 == 0 else "_IMPAR"
                classe = "" if type(row["change_value"]) == str else "verde" if row["change_value"] > 0 else "vermelho"
                # formantando change value na para posicionar o sinal de - corretamente
                change_value = "R$-" if type(row["change_value"]) == str else "R$"+str(row["change_value"]) if row["change_value"] > 0 else "-R$"+str(abs(row["change_value"]))
                total = "-" if type(row["total"]) == str else int(row["total"])
                html += f'''
                    <tr class="{row["granularidade"]+impar_ou_par}">
                        <td> {row["granularidade"]}</td>
                        <td> {row["produto"]}</td>
                        <td> {"R$"+str(row["preco_fechamento_anterior"]).replace(".", ",")}</td>
                        <td> {row["datetime_fechamento_anterior"]}</td>
                        <td> {"R$"+str(row["close"]).replace(".", ",")}</td>
                        <td> {row["hora_fechamento"]}</td>
                        <td> {str(row["volume"]).replace(".", ",")}</td>
                        <td> {total}</td>
                        <td class="{classe}"> {str(change_value).replace(".", ",")}</td>
                        <td class="{classe}"> {str(row["change_percent"]).replace(".", ",") +"%"}</td>
                        <td> {"R$"+str(row["open"]).replace(".", ",")}</td>
                        <td> {"R$"+str(row["high"]).replace(".", ",")}</td>
                        <td> {"R$"+str(row["low"]).replace(".", ",")}</td>
                    </tr>
                '''
            html +=f'''
                </tbody>
                </table>'''
        html = style+html
        return {"html":html}

class CategoriaNegociacao:
    tb:db.Table = __DB__.getSchema('tb_categoria_negociacao')
    @staticmethod
    def get_all():
        __DB__.connect()
        query = db.select(
            CategoriaNegociacao.tb.c['id'],
            CategoriaNegociacao.tb.c['nome']
        )
        result = __DB__.db_execute(query, commit=prod)
        df = pd.DataFrame(result, columns=['id', 'nome'])
        return df.to_dict("records")
    
if __name__ == "__main__":
    # print(ProdutoInteresse.get_all())
    # print(NegociacoesResumo.get_negociacoes_interesse_por_data(datetime.date.today()))
    print(CategoriaNegociacaoEnum("Mesa").name)
    # print(NegociacoesResumo.get_html_table_negociacoes_bbce(datetime.date.today(), None)["html"])
    pass