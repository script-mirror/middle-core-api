import sqlalchemy as sa
import pandas as pd
from typing import List, Dict, Any

from app.core.database.wx_dbClass import db_mysql_master

__DB__ = db_mysql_master('db_meteorologia')


class EstacaoChuvosaObservada:
    # Define tables at class level - they'll be assigned in the methods since they depend on parameters
    tb_ec_norte = __DB__.getSchema('tb_chuva_observada_estacao_chuvosa_norte')
    tb_ec_sudeste = __DB__.getSchema('tb_chuva_observada_estacao_chuvosa')
    tb_cadastro_ec_norte = __DB__.getSchema(
        'tb_cadastro_estacao_chuvosa_norte')
    tb_cadastro_ec_sudeste = __DB__.getSchema('tb_cadastro_estacao_chuvosa')
    tb_chuva_prevista_ec_norte = __DB__.getSchema(
        'tb_chuva_prevista_estacao_chuvosa_norte')
    tb_chuva_prevista_ec_sudeste = __DB__.getSchema(
        'tb_chuva_prevista_estacao_chuvosa')

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
            tb_cadastro_estacao_chuvosa.c.str_modelo == modelo
        ).where(
            tb_cadastro_estacao_chuvosa.c.dt_rodada == dt_rodada
        ).where(
            tb_cadastro_estacao_chuvosa.c.hr_rodada == hr_rodada
        )

        id = __DB__.db_execute(select).fetchall()[0][-1]

        select_values = sa.select(
            tb_chuva_prevista_estacao_chuvosa.c.dt_prevista,
            tb_chuva_prevista_estacao_chuvosa.c.vl_chuva,
        ).where(
            tb_chuva_prevista_estacao_chuvosa.c.id_cadastro == id
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
        df_climatologia = pd.DataFrame(
            vl_chuva, columns=["bacia", "time", "climatologia"])

        return df_climatologia.to_dict('records')


class VentoPrevistoWEOL:

    tb_cadastro_vento_previsto = __DB__.getSchema('tb_cadastro_vento_previsto')
    tb_valores_vento_previsto = __DB__.getSchema('tb_valores_vento_previsto')

    @staticmethod
    def insert_vento_previsto(valores: list, dt_rodada: str, hr_rodada: int, modelo: str):
        """
        Insere os dados de vento previsto:
        - Remove cadastro anterior da mesma rodada e modelo
        - Cria um novo cadastro
        - Insere os valores vinculados ao novo cadastro
        """

        # Remove cadastro anterior (se existir)
        query_delete = sa.delete(VentoPrevistoWEOL.tb_cadastro_vento_previsto).where(
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.dt_rodada == dt_rodada,
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.hr_rodada == hr_rodada,
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.str_modelo == modelo
        )
        __DB__.db_execute(query_delete)

        # Insere novo cadastro
        query_insert = VentoPrevistoWEOL.tb_cadastro_vento_previsto.insert().values(
            dt_rodada=dt_rodada,
            hr_rodada=hr_rodada,
            str_modelo=modelo
        )
        id_cadastro = __DB__.db_execute(query_insert).inserted_primary_key[0]

        # Prepara os valores para inserir
        df_valores = pd.DataFrame(valores)
        df_valores = df_valores.to_dict(orient='records')
        df_valores = [{v[0]: v[1] for v in item.values()}
                      for item in df_valores]
        df_valores = pd.DataFrame(df_valores)
        df_valores['id_cadastro'] = id_cadastro
        print(df_valores)

        # Insere os valores na tabela
        query_insert_valores = VentoPrevistoWEOL.tb_valores_vento_previsto.insert().values(
            df_valores.to_dict(orient='records')
        )
        __DB__.db_execute(query_insert_valores)

        return {
            'id_cadastro': id_cadastro,
            'n_registros_inseridos': len(df_valores)
        }

    @staticmethod
    def get_rodadas(dt_rodada: str = None, hr_rodada: int = None, modelo: str = None):
        """
        Obtém as rodadas de vento previsto.
        Se nenhum parâmetro for fornecido, retorna todas as rodadas.
        """

        query = sa.select(
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.dt_rodada,
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.hr_rodada,
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.str_modelo,
        )

        if dt_rodada:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.dt_rodada == dt_rodada)
        if hr_rodada:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.hr_rodada == hr_rodada)
        if modelo:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.str_modelo == modelo)

        results = __DB__.db_execute(query).fetchall()
        df_results = pd.DataFrame(results, columns=[
            'dt_rodada', 'hr_rodada', 'modelo'
        ])

        return df_results.to_dict('records')

    @staticmethod
    def get_vento_previsto(dt_rodada: str = None, hr_rodada: int = None, modelo: str = None):
        """
        Obtém os dados de vento previsto para uma rodada específica.
        Se nenhum parâmetro for fornecido, retorna todos os registros.
        """

        query = sa.select(
            VentoPrevistoWEOL.tb_valores_vento_previsto.c.dt_prevista,
            VentoPrevistoWEOL.tb_valores_vento_previsto.c.vl_vento,
            VentoPrevistoWEOL.tb_valores_vento_previsto.c.estado,
            VentoPrevistoWEOL.tb_valores_vento_previsto.c.aglomerado,
        ).select_from(
            VentoPrevistoWEOL.tb_cadastro_vento_previsto.join(
                VentoPrevistoWEOL.tb_valores_vento_previsto,
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.id == VentoPrevistoWEOL.tb_valores_vento_previsto.c.id_cadastro
            )
        )

        if dt_rodada:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.dt_rodada == dt_rodada)
        if hr_rodada:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.hr_rodada == hr_rodada)
        if modelo:
            query = query.where(
                VentoPrevistoWEOL.tb_cadastro_vento_previsto.c.str_modelo == modelo)

        results = __DB__.db_execute(query).fetchall()
        df_results = pd.DataFrame(results, columns=[
            'dt_prevista', 'vl_vento', 'estado', 'aglomerado'
        ])

        return df_results.to_dict('records')

class IndicesSST:

    tb_indices_diarios_sst = __DB__.getSchema('tb_indices_diarios_sst')

    @staticmethod
    def insert_indices_sst(valores) -> Dict[str, Any]:
        """
        Insere os dados de índices de temperatura da superfície do mar (SST).
        """
        if not valores:
            return {'message': 'Nenhum valor fornecido para inserção.'}

        # Converte objetos em dicionários
        try:
            # Caso valores sejam instâncias de uma classe
            lista_dicts = [v.__dict__ for v in valores]
        except AttributeError:
            return {'message': 'Os itens em "valores" devem ser objetos com atributos acessíveis por __dict__.'}

        # Converte para DataFrame
        df_valores = pd.DataFrame(lista_dicts)

        dt_observada = df_valores['dt_observada'].iloc[0] if 'dt_observada' in df_valores.columns else None
        str_indice = df_valores['str_indice'].iloc[0] if 'str_indice' in df_valores.columns else None
        # Remove cadastro anterior (se existir)
        query_delete = sa.delete(IndicesSST.tb_indices_diarios_sst).where(
            IndicesSST.tb_indices_diarios_sst.c.dt_observada == dt_observada,
            IndicesSST.tb_indices_diarios_sst.c.str_indice == str_indice
        )
        __DB__.db_execute(query_delete)

        # (Opcional) Valida e converte dt_observada
        df_valores['dt_observada'] = pd.to_datetime(df_valores['dt_observada'], errors='coerce')
        if df_valores['dt_observada'].isnull().any():
            return {'message': 'Uma ou mais datas são inválidas.'}

        # Insere os valores na tabela
        query_insert = IndicesSST.tb_indices_diarios_sst.insert().values(df_valores.to_dict(orient='records'))
        __DB__.db_execute(query_insert)

        return {'message': f'{len(df_valores)} registros inseridos com sucesso.'}
    
    @staticmethod
    def get_indices_sst(dt_inicio: str = None, dt_fim: str = None) -> List[Dict[str, Any]]:
        """
        Obtém os dados de índices de temperatura da superfície do mar (SST) em um intervalo de datas.
        """
        query = sa.select(
            IndicesSST.tb_indices_diarios_sst.c.dt_observada,
            IndicesSST.tb_indices_diarios_sst.c.vl_indice
        )

        if dt_inicio:
            query = query.where(IndicesSST.tb_indices_diarios_sst.c.dt_observada >= dt_inicio)
        if dt_fim:
            query = query.where(IndicesSST.tb_indices_diarios_sst.c.dt_observada <= dt_fim)

        results = __DB__.db_execute(query).fetchall()
        df_results = pd.DataFrame(results, columns=[
            'dt_observada', 'vl_indice'
        ])

        return df_results.to_dict('records')