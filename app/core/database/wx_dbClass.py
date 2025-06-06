import os
import sqlalchemy as db
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.message.service import send_message

class db_mysql_master():


    def __init__(self, dbase):
        __host_mysql = settings.host_mysql
        __port_mysql = settings.port_mysql

        __user_mysql = settings.user_mysql
        __password_mysql = settings.password_mysql

        self.meta = db.MetaData()
        self.user = __user_mysql
        self.password = __password_mysql
        self.host = __host_mysql
        self.port = __port_mysql
        self.dbase = dbase

        
        self.engine= db.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{dbase}", pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    def connect(self):
        return self.Session()

    def db_execute(self, query, commit:bool=True, debug:str=None):
        session = self.connect()
        try:
            result = session.execute(query)
            if commit:
                session.commit()
            return result
        except Exception as e:
            if commit:
                session.rollback()
            send_message(f"Error executing query: {str(e.orig)}\nQuery: {str(query)}\n{debug}", file=None, dest="debug")            
            raise Exception(f"Error executing query: {str(e.orig)}") from e
        finally:
            session.close()
    
    def create_table(self, table_name):
        self.getSchema(table_name)
        self.meta.create_all(self.engine)
    
    
    def get_conn(self):
        conn: Connection = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()        

    def getSchema(self, table_name:str) -> db.Table:
        
        if table_name.lower() == 'tb_cadastro_rodadas':
            table_schema = db.Table('tb_cadastro_rodadas', self.meta,
                db.Column('id', db.INTEGER, primary_key=True),
                db.Column('id_chuva', db.INTEGER),
                db.Column('id_smap', db.INTEGER),
                db.Column('id_previvaz', db.INTEGER),
                db.Column('id_prospec', db.INTEGER),
                db.Column('dt_rodada', db.Date, index=True),
                db.Column('hr_rodada', db.SmallInteger),
                db.Column('str_modelo', db.String(250)),
                db.Column('fl_preliminar', db.Boolean),
                db.Column('fl_pdp', db.Boolean),
                db.Column('fl_psat', db.Boolean),
                db.Column('fl_estudo', db.Boolean),
                db.Column('dt_revisao', db.Date),
            )

        elif table_name.lower() == 'tb_chuva':
            table_schema = db.Table('tb_chuva', self.meta,
                db.Column('id', db.Integer),
                db.Column('cd_subbacia', db.Integer),
                db.Column('dt_prevista', db.Date),
                db.Column('vl_chuva', db.Float),
            )

        elif table_name.lower() == 'tb_smap':
            table_schema = db.Table('tb_smap', self.meta,
                db.Column('id', db.Integer),
                db.Column('cd_posto', db.Integer),
                db.Column('dt_prevista', db.Date),
                db.Column('vl_vazao_vna', db.Float),
                db.Column('vl_vazao_prevs', db.Float),                
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_prevs':
            table_schema = db.Table('tb_prevs', self.meta,
                db.Column('id', db.Integer),
                db.Column('cd_posto', db.Integer),
                db.Column('dt_prevista', db.Date),
                db.Column('vl_vazao', db.Float),
            )
            
        elif table_name.lower() == 'tb_subbacia':
            table_schema = db.Table('tb_subbacia', self.meta,
                db.Column('cd_subbacia', db.Integer),
                db.Column('txt_nome_subbacia', db.String(100)),
                db.Column('txt_submercado', db.String(20)),
                db.Column('txt_bacia', db.String(100)),
                db.Column('vl_lat', db.Float),
                db.Column('vl_lon', db.Float),
                db.Column('txt_nome_smap', db.String(150)),
                db.Column('txt_pasta_contorno', db.String(150)),
                db.Column('cd_bacia_mlt', db.Integer)
            )
            
        elif table_name.lower() == 'tb_palavras_pauta_aneel':
            table_schema = db.Table('tb_palavras_pauta_aneel',self.meta,    
				db.Column("id", db.Integer, primary_key=True),
				db.Column("palavra",db.String(70)),  
				db.Column("tag", db.String(50))
				)
        
        elif table_name.lower() == 'tb_produtos':
            table_schema = db.Table('tb_produtos', self.meta, 
                db.Column('id_produto', db.Integer), 
                db.Column('str_produto', db.String(50)),
                db.Column('dt_cria', db.DATETIME),
            )

        elif table_name.lower() == 'tb_negociacoes':
            table_schema = db.Table('tb_negociacoes', self.meta,
                db.Column('id_negociacao', db.Integer),
                db.Column('id_produto', db.Integer),
                db.Column('vl_quantidade', db.Integer),
                db.Column('vl_preco', db.Float),
                db.Column('dt_criacao', db.DATETIME),
                db.Column('id_categoria_negociacao', db.ForeignKey('bbce.tb_categoria_negociacao.id'))
            )

        elif table_name.lower() == 'tb_produtos_bbce':
            table_schema = db.Table('tb_produtos_bbce',self.meta,    
            db.Column("id", db.Integer, primary_key=True),
            db.Column("str_produto",db.String(70))
            )

        elif table_name.lower() == 'tb_processos_aneel':
            table_schema = db.Table('tb_processos_aneel',self.meta,    
            db.Column("id", db.Integer, primary_key=True),
            db.Column("str_processo",db.String(70)),
            db.Column("str_efeito",db.String(70)),
            db.Column("str_assunto",db.String(1024)),
            db.Column("str_ultimo_documento_enviado",db.String(70)),
            db.Column("str_relator",db.String(70)),
            db.Column("str_superintendencia",db.String(70))            
            )
                
        elif table_name.lower() == 'tb_assuntos_aneel':
            table_schema = db.Table('tb_assuntos_aneel',self.meta,    
            db.Column("id", db.Integer, primary_key=True),
            db.Column("str_assunto",db.String(70)),
            )
                
        elif  table_name.lower() == 'tb_cadastro_newave': 
            table_schema = db.Table('tb_cadastro_newave', self.meta,
            db.Column('id', db.Integer,primary_key=True,autoincrement=True),
            db.Column('dt_inicio_rv',db.Date), 
            db.Column('id_fonte',db.Integer),
            db.Column('tx_comentario',db.VARCHAR(250))
            )
            
    
        elif  table_name.lower() == 'tb_fontes': 
            table_schema = db.Table('tb_fontes', self.meta,
            db.Column('cd_fonte', db.Integer,primary_key=True,autoincrement=True),
            db.Column('str_fonte',db.VARCHAR(100))
        )
            
        elif  table_name.lower() == 'tb_nw_carga': 
            table_schema = db.Table('tb_nw_carga', self.meta,
            db.Column('id_deck', db.Integer),
            db.Column('dt_referente',db.Date), 
            db.Column('cd_submercado',db.Integer),
            db.Column('vl_carga',db.FLOAT),
            db.Column('Exp_CGH',db.FLOAT),
            db.Column('Exp_EOL',db.FLOAT),
            db.Column('Exp_UFV',db.FLOAT),
            db.Column('Exp_UTE',db.FLOAT),
            db.Column('Exp_MMGD',db.FLOAT)
            )
                        
                        
        elif table_name.lower() == 'tb_cadastro_decomp':
            table_schema = db.Table('tb_cadastro_decomp',self.meta,    
            db.Column("id", db.Integer),
            db.Column("dt_inicio_rv",db.DATE),
            db.Column("id_fonte",db.VARCHAR(100)),
            db.Column("tx_comentatio",db.VARCHAR(100)),
            
            )
            
        elif table_name.lower() == 'tb_ds_bloco_tm':    
            table_schema = db.Table('tb_ds_bloco_tm', self.meta,
                        db.Column('dt_inicio_patamar', db.DATETIME),
                        db.Column('vl_patamar', db.Integer)
            )
            
        elif table_name.lower() == 'tb_dc_dadger_dp':
            table_schema = db.Table('tb_dc_dadger_dp',self.meta,    
            db.Column("id_deck", db.Integer),
            db.Column("ip",db.Integer),
            db.Column("vl_carga_pesada",db.FLOAT),
            db.Column("vl_carga_media",db.FLOAT),
            db.Column("vl_carga_leve",db.FLOAT),
            db.Column("cd_submercado",db.Integer),
            
            )
        elif table_name.lower() == 'tb_dc_patamar':
            table_schema = db.Table('tb_dc_patamar',self.meta,    
            db.Column("id_deck", db.Integer),
            db.Column("ip",db.Integer),
            db.Column("hr_p1",db.FLOAT),
            db.Column("hr_p2",db.FLOAT),
            db.Column("hr_p3",db.FLOAT),
            )
            
        elif table_name.lower() == 'tb_ds_carga':
            table_schema = db.Table('tb_ds_carga', self.meta,
            db.Column('id_deck', db.Integer),
            db.Column('cd_submercado', db.Integer),
            db.Column('dataHora',db.DATETIME),
            db.Column('vl_carga', db.FLOAT),
            )
            
        elif table_name.lower() == 'tb_cadastro_dessem':
            table_schema = db.Table('tb_cadastro_dessem', self.meta,
            db.Column('id', db.Integer),
            db.Column('dt_rodada', db.DATE),
            db.Column('dt_referente', db.DATE),
            db.Column('id_fonte', db.Integer()),
            db.Column('tx_comentatio', db.VARCHAR(100)))
            
        elif table_name.lower() == 'tb_pld':
            table_schema = db.Table('tb_pld', self.meta,
            db.Column('str_ano', db.SMALLINT,nullable=False),
            db.Column('vl_PLDmax_hora', db.FLOAT,nullable=False),
            db.Column('vl_PLDmax_estr', db.FLOAT,nullable=False),
            db.Column('vl_PLDmin', db.FLOAT,nullable=False),
            db.Column('vl_custo_Deficit', db.FLOAT,nullable=False)
            )
            
        elif table_name.lower() == 'tb_intercambio_dessem':
            table_schema = db.Table('tb_intercambio_dessem',self.meta,
            db.Column('vl_restricao', db.FLOAT),
            db.Column('str_nomeRE', db.String(250),primary_key=True,nullable=False),
            db.Column('dt_data_hora', db.DATETIME,primary_key=True,nullable=False),
            db.Column('vl_limite_inferior', db.INTEGER), 
            db.Column('vl_limite_utilizado', db.INTEGER),
            db.Column('vl_limite_superior',db.INTEGER),
            db.PrimaryKeyConstraint('str_nomeRE','dt_data_hora'),
            )
            
        elif table_name.lower() == 'tb_balanco_dessem':
            table_schema = db.Table('tb_balanco_dessem', self.meta,
            db.Column('dt_data_hora', db.DATETIME,primary_key=True,nullable=False),
            db.Column('str_subsistema',db.String(2),primary_key=True,nullable=False),
            db.Column('vl_cmo', db.FLOAT),
            db.Column('vl_demanda', db.FLOAT),
            db.Column('vl_geracao_renovaveis', db.FLOAT),
            db.Column('vl_geracao_hidreletrica', db.FLOAT),
            db.Column('vl_geracao_termica', db.FLOAT),
            db.Column('vl_gtmin', db.FLOAT),
            db.Column('vl_gtmax', db.FLOAT),
            db.Column('vl_intercambio', db.FLOAT),
            db.Column('vl_pld', db.FLOAT),
            db.PrimaryKeyConstraint('dt_data_hora','str_subsistema'),
            )
            
        elif table_name.lower() == 'tb_carga_ipdo':
            table_schema = db.Table('tb_carga_ipdo', self.meta,
            db.Column('dt_referente',db.DATE),
            db.Column('carga_se', db.FLOAT),
            db.Column('carga_s', db.FLOAT),
            db.Column('carga_ne', db.FLOAT),
            db.Column('carga_n', db.FLOAT)
            )
            
        elif table_name.lower() == 'tb_ve_bacias':
            table_schema = db.Table('tb_ve_bacias', self.meta,
                db.Column('vl_ano', db.Integer),
                db.Column('vl_mes', db.Integer),
                db.Column('cd_revisao', db.Integer),
                db.Column('cd_bacia', db.Integer),
                db.Column('dt_inicio_semana', db.DateTime),
                db.Column('vl_ena', db.Float),
                db.Column('vl_perc_mlt', db.Float),
                extend_existing=True
            )
        
        elif table_name.lower() == 'tb_ve':
            table_schema = db.Table('tb_ve', self.meta,
                db.Column('vl_ano', db.Integer),
                db.Column('vl_mes', db.Integer),
                db.Column('cd_revisao', db.Integer),
                db.Column('cd_submercado', db.Integer),
                db.Column('dt_inicio_semana', db.DateTime),
                db.Column('vl_ena', db.Float)
            )

        elif table_name.lower() == 'tb_submercado':
            table_schema = db.Table('tb_submercado', self.meta,
                db.Column('cd_submercado', db.Integer, primary_key=True),
                db.Column('str_submercado', db.VARCHAR(100)),
                db.Column('str_sigla', db.VARCHAR(100))
            )

        elif table_name.lower() == 'tb_bacias':
            
            table_schema = db.Table('tb_bacias', self.meta,
                db.Column('id_bacia', db.INTEGER, primary_key=True),
                # db.Column('id_submercado', db.INTEGER),
                db.Column('str_bacia', db.VARCHAR(100)),
            )
            
        elif table_name.lower() == 'tb_dc_dadger_pq':
            table_schema = db.Table('tb_dc_dadger_pq',self.meta,    
            db.Column("id_deck", db.Integer,primary_key=True,nullable=False),
            db.Column("vl_estagio",db.Integer,nullable=False),
            db.Column("vl_patamar",db.Integer,nullable=False),
            db.Column("cd_submercado",db.Integer,nullable=False),
            db.Column("vl_geracao_pct",db.FLOAT),
            db.Column("vl_geracao_pch",db.FLOAT),
            db.Column("vl_geracao_eol",db.FLOAT),
            db.Column("vl_geracao_ufv",db.FLOAT),
            )
        elif table_name.lower() == 'tb_pluvia_ena':
            table_schema = db.Table('tb_pluvia_ena', self.meta,
                db.Column('ID_RODADA', db.Integer, primary_key=True),
                db.Column('CD_BACIA', db.Integer, nullable=False),
                db.Column('VL_ENA', db.Float, nullable=False),
                db.Column('VL_ENA_PERC_MLT', db.Float, nullable=False),
                db.Column('DT_REFERENTE', db.Date),
                db.Column('STR_MAPA', db.String(100)),
                db.Column('DT_RODADA', db.Date, nullable=False),
                db.Column('VL_MEMBRO', db.String(100)),
                db.Column('FL_VIES', db.Boolean, default=False),
                db.Column('FL_PRELIMINAR', db.Boolean, default=False),
                db.Column('STR_MODELO', db.String(100))
            )
            
        elif table_name.lower() == 'vw_perfil_semanal':
            table_schema = db.Table('vw_perfil_semanal',self.meta,
            db.Column("mes", db.Integer),
            db.Column("nome_dia_semana", db.Integer),
            db.Column("resultado_carga_se", db.FLOAT),
            db.Column("resultado_carga_s", db.FLOAT),
            db.Column("resultado_carga_ne", db.FLOAT),
            db.Column("resultado_carga_n", db.FLOAT),
            )

        elif table_name.lower() == 'tb_chuva_obs':
            table_schema = db.Table('tb_chuva_obs', self.meta,
                db.Column('cd_subbacia', db.Integer),
                db.Column('dt_observado', db.Date),
                db.Column('vl_chuva', db.Float)
            )
        elif table_name.lower() == 'tb_acomph':
            table_schema = db.Table('tb_acomph', self.meta,
                db.Column('dt_referente', db.DateTime),
                db.Column('cd_posto', db.SmallInteger),
                db.Column('vl_ear_lido', db.Float),
                db.Column('vl_ear_conso', db.Float),
                db.Column('vl_vaz_def_lido', db.Float),
                db.Column('vl_vaz_def_conso', db.Float),
                db.Column('vl_vaz_afl_lido', db.Float),
                db.Column('vl_vaz_afl_conso', db.Float),
                db.Column('vl_vaz_inc_conso', db.Float),
                db.Column('vl_vaz_nat_conso', db.Float),
                db.Column('dt_acomph', db.DateTime)
            )

        elif table_name.lower() == 'tb_ree':
                table_schema = db.Table('tb_ree', self.meta,
                db.Column('cd_ree', db.SmallInteger, primary_key=True),
                db.Column('str_ree', db.String(255)),
                db.Column('cd_submercado', db.Integer)
            )

        elif table_name.lower() == 'tb_rdh':
            table_schema = db.Table('tb_rdh', self.meta,
                db.Column('cd_posto', db.SmallInteger),
                db.Column('vl_vol_arm_perc', db.Float),
                db.Column('vl_mlt_vaz', db.Float),
                db.Column('vl_vaz_dia', db.Float),
                db.Column('vl_vaz_turb', db.Float),
                db.Column('vl_vaz_vert', db.Float),
                db.Column('vl_vaz_dfl', db.Float),
                db.Column('vl_vaz_transf', db.Float),
                db.Column('vl_vaz_afl', db.Float),
                db.Column('vl_vaz_inc', db.Float),
                db.Column('vl_vaz_consunt', db.Float),
                db.Column('vl_vaz_evp', db.Float),
                db.Column('dt_referente', db.DateTime)
            )

        elif table_name.lower() == 'tb_rdh_submercado':
            table_schema = db.Table('tb_rdh_submercado', self.meta,
                db.Column('cd_submercado', db.SmallInteger),
                db.Column('vl_vol_arm_perc', db.Float),
                db.Column('vl_media_mes_65', db.Float),
                db.Column('vl_media_semana_65', db.Float),
                db.Column('vl_media_mes_queda', db.Float),
                db.Column('vl_media_semana_queda', db.Float),
                db.Column('dt_referente', db.DateTime)
            )

        elif table_name.lower() == 'tb_rdh_ree':
            table_schema = db.Table('tb_rdh_ree', self.meta,
                db.Column('cd_ree', db.SmallInteger),
                db.Column('vl_vol_arm_perc', db.Float),
                db.Column('vl_media_mes_65', db.Float),
                db.Column('vl_media_semana_65', db.Float),
                db.Column('vl_media_mes_queda', db.Float),
                db.Column('vl_media_semana_queda', db.Float),
                db.Column('dt_referente', db.DateTime)
            )

        elif table_name.lower() == 'tb_chuva_psat':
            table_schema = db.Table('tb_chuva_psat', self.meta,
                db.Column('cd_subbacia', db.Integer),
                db.Column('dt_ini_observado', db.Date),
                db.Column('vl_chuva', db.Float)
            )

        elif table_name.lower() == 'tb_earm_max':
            table_schema = db.Table('tb_earm_max', self.meta,
                db.Column('str_submercado', db.String(100)),
                db.Column('vl_earm_max', db.Float),
            )

        elif table_name.lower() == 'tb_weol_eolica':
            table_schema = db.Table('tb_weol_eolica', self.meta,
                db.Column('id', db.INTEGER, primary_key=True, autoincrement=True),
                db.Column('cd_submercado', db.SmallInteger),
                db.Column('vl_geracao_eol', db.Float),
                db.Column('dt_referente', db.DATETIME),
                db.Column('dt_deck', db.Date),
            )

        elif table_name.lower() == 'tb_tmp_smap':
            table_schema = db.Table('tb_tmp_smap', self.meta,
                    db.Column('txt_subbacia', db.String(100)),
                    db.Column('cd_estacao', db.Integer),
                    db.Column('txt_tipo_vaz', db.String(100)),
                    db.Column('dt_referente', db.DateTime),
                    db.Column('vl_vaz', db.Float)
                )
        elif table_name.lower() == 'tb_prev_carga_temperatura':
            table_schema = db.Table('tb_prev_carga_temperatura', self.meta,
                    db.Column('cd_submercado', db.SmallInteger),
                    db.Column('dt_referente', db.DateTime),
                    db.Column('vl_temperatura', db.Float),
                    db.Column('dt_inicio', db.DateTime),
                )

        elif table_name.lower() == 'tb_temperatura_obs':
            table_schema = db.Table('tb_temperatura_obs', self.meta,
                    db.Column('cd_submercado', db.SmallInteger),
                    db.Column('dt_referente', db.DateTime),
                    db.Column('vl_temperatura', db.Float),
                )

        elif table_name.lower() == 'tb_prev_carga':

            table_schema = db.Table('tb_prev_carga', self.meta,
                    db.Column('cd_submercado', db.SmallInteger),
                    db.Column('dt_referente', db.DateTime),
                    db.Column('vl_carga', db.Float),
                    db.Column('dt_inicio', db.DateTime),
                )
            
       
        elif table_name.lower() == 'tb_ena_submercado':
            
            table_schema = db.Table('tb_ena_submercado', self.meta,
                db.Column('id_submercado', db.INTEGER),
                db.Column('dt_ref', db.Date),
                db.Column('vl_mwmed', db.Float),
                db.Column('vl_percent', db.Float),
            )

        elif table_name.lower() == 'tb_ena_bacia':
            
            table_schema = db.Table('tb_ena_bacia', self.meta,
                db.Column('id_bacia', db.INTEGER),
                db.Column('dt_ref', db.Date),
                db.Column('vl_mwmed', db.Float),
                db.Column('vl_percent', db.Float),
            )   
    
        elif table_name.lower() == 'tb_nw_sist_intercambio':

            table_schema = db.Table('tb_nw_sist_intercambio', self.meta,
                    db.Column('vl_ano', db.SmallInteger),
                    db.Column('vl_mes', db.SmallInteger),
                    db.Column('cd_submerc_src', db.SmallInteger),
                    db.Column('cd_submerc_dst', db.SmallInteger),
                    db.Column('vl_intercambio', db.Float),
                    db.Column('dt_deck', db.DateTime),

                )
        
        elif table_name.lower() == 'tb_nw_sist_energia':

            table_schema = db.Table('tb_nw_sist_energia', self.meta,
                    db.Column('cd_submercado', db.SmallInteger),
                    db.Column('vl_ano', db.SmallInteger),
                    db.Column('vl_mes', db.SmallInteger),
                    db.Column('vl_energia_total',  db.Float),
                    db.Column('vl_geracao_pch', db.Float),
                    db.Column('vl_geracao_pct', db.Float),
                    db.Column('vl_geracao_eol', db.Float),
                    db.Column('vl_geracao_ufv', db.Float),
                    db.Column('vl_geracao_pch_mmgd', db.Float),
                    db.Column('vl_geracao_pct_mmgd', db.Float),
                    db.Column('vl_geracao_eol_mmgd', db.Float),
                    db.Column('vl_geracao_ufv_mmgd', db.Float),
                    db.Column('dt_deck', db.DateTime),

                )
            
        elif table_name.lower() == 'tb_nw_cadic':

            table_schema = db.Table('tb_nw_cadic', self.meta,
                    db.Column('vl_ano', db.SmallInteger),
                    db.Column('vl_mes', db.SmallInteger),
                    db.Column('vl_const_itaipu', db.SmallInteger),
                    db.Column('vl_ande', db.Float),
                    db.Column('vl_mmgd_se', db.Float),
                    db.Column('vl_mmgd_s', db.Float),
                    db.Column('vl_mmgd_ne', db.Float),
                    db.Column('vl_boa_vista', db.Float),
                    db.Column('vl_mmgd_n', db.Float),
                    db.Column('dt_deck', db.DateTime),
                )
            
        
        elif table_name.lower() == 'tb_postos_completo':

            table_schema = db.Table('tb_postos_completo', self.meta,
                db.Column('cd_posto', db.SmallInteger),
                db.Column('str_posto', db.String(100)),
                db.Column('cd_bacia', db.SmallInteger),
                db.Column('cd_ree', db.SmallInteger),
                db.Column('cd_submercado', db.SmallInteger),
                
            )

        elif table_name.lower() == 'tb_bacias_segmentadas':
            table_schema = db.Table('tb_bacias_segmentadas', self.meta,
                db.Column('cd_bacia', db.Integer, primary_key=True),
                db.Column('str_bacia', db.VARCHAR(100)),
                db.Column('cd_submercado', db.SmallInteger)
            )

        elif table_name.lower() == 'tb_ds_renovaveis':
            table_schema = db.Table('tb_ds_renovaveis', self.meta,
                db.Column('id_deck', db.Integer),
                db.Column('vl_periodo', db.Integer, nullable=False),
                db.Column('id_submercado', db.Integer, nullable=False),
                db.Column('vl_geracao_uhe', db.Float),
                db.Column('vl_geracao_ute', db.Float),
                db.Column('vl_geracao_cgh', db.Float),
                db.Column('vl_geracao_pch', db.Float),
                db.Column('vl_geracao_ufv', db.Float),
                db.Column('vl_geracao_uee', db.Float),
                db.Column('vl_geracao_mgd', db.Float),
            )

        elif table_name.lower() == 'tb_ds_pdo_cmosist':
            table_schema = db.Table('tb_ds_pdo_cmosist', self.meta,
                db.Column('id_deck', db.Integer),
                db.Column('cd_submercado', db.SmallInteger),
                db.Column('vl_horario', db.Time),
                db.Column('vl_preco', db.Float)
            )
            
        # db_meteorologia
        elif table_name.lower() == 'tb_inmet_estacoes':
            table_schema = db.Table('tb_inmet_estacoes', self.meta,
                db.Column('cd_estacao', db.VARCHAR(4)),
                db.Column('str_nome', db.VARCHAR(50)),
                db.Column('str_estado', db.VARCHAR(2)),
                db.Column('vl_lat', db.Float),
                db.Column('vl_lon', db.Float),
                db.Column('str_bacia', db.VARCHAR(50)),
                db.Column('str_fonte', db.VARCHAR(50)),

            )

        elif table_name.lower() == 'tb_inmet_dados_estacoes':
            table_schema = db.Table('tb_inmet_dados_estacoes', self.meta,
                db.Column('cd_estacao', db.VARCHAR(4)),
                db.Column('dt_coleta', db.DateTime),
                db.Column('vl_chuva', db.Float),
            )

        elif table_name.lower() == 'tb_prev_ena_submercado':

            table_schema = db.Table('tb_prev_ena_submercado', self.meta,
                db.Column('cd_submercado', db.SmallInteger),
                db.Column('dt_previsao', db.Date),
                db.Column('dt_ref', db.Date),
                db.Column('vl_mwmed', db.Float),
                db.Column('vl_perc_mlt', db.Float)
            )
        elif table_name.lower() == 'tb_estado':
                table_schema = db.Table('tb_estado', self.meta,
                db.Column('id_estado', db.Integer),
                db.Column('str_estado', db.String),
                db.Column('str_sigla', db.String),
                db.Column('id_submercado', db.ForeignKey('db_ons.tb_submercado.cd_submercado')),
                extend_existing=True   
            )
        elif table_name.lower() == 'tb_restricoes_coff_eolica':
            
            table_schema = db.Table('tb_restricoes_coff_eolica', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('cd_razao_restricao', db.String, primary_key=True),
                db.Column('vl_geracao', db.Float),
                db.Column('vl_geracao_limitada', db.Float),
                db.Column('vl_disponibilidade', db.Float),
                db.Column('vl_geracao_referencia', db.Float),
                db.Column('vl_geracao_referencia_final', db.Float),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_restricoes_coff_solar':
            
            table_schema = db.Table('tb_restricoes_coff_solar', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('cd_razao_restricao', db.String, primary_key=True),
                db.Column('vl_geracao', db.Float),
                db.Column('vl_geracao_limitada', db.Float),
                db.Column('vl_disponibilidade', db.Float),
                db.Column('vl_geracao_referencia', db.Float),
                db.Column('vl_geracao_referencia_final', db.Float),
                extend_existing=True
            )    

        elif table_name.lower() == 'tb_niveis_dessem':
            table_schema = db.Table('tb_niveis_dessem', self.meta,
                db.Column('dt_referente', db.DateTime),
                db.Column('cd_uhe', db.SmallInteger),
                db.Column('vl_vaz_defl', db.Float),
                db.Column('vl_perc_vaz_defl', db.Float)
            )

        elif table_name.lower() == 'tb_posto_uhe':
            table_schema = db.Table('tb_posto_uhe', self.meta,
                db.Column('cd_uhe', db.SmallInteger, nullable=True),
                db.Column('cd_posto', db.SmallInteger, nullable=True),
                db.Column('cd_np', db.String(10), nullable=True),
                db.Column('str_usina', db.String(255), nullable=True),
                db.Column('cd_submercado', db.SmallInteger, nullable=True)
            )
            
        elif table_name.lower() == 'tb_geracao_usina_eolica':
            
            table_schema = db.Table('tb_geracao_usina_eolica', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_geracao', db.Float),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_geracao_usina_hidraulica':
            
            table_schema = db.Table('tb_geracao_usina_hidraulica', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_geracao', db.Float),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_geracao_usina_nuclear':
            
            table_schema = db.Table('tb_geracao_usina_nuclear', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_geracao', db.Float),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_geracao_usina_solar':
            
            table_schema = db.Table('tb_geracao_usina_solar', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_geracao', db.Float),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_geracao_usina_termica':
            
            table_schema = db.Table('tb_geracao_usina_termica', self.meta,
                db.Column('id_estado', db.ForeignKey('db_ons_dados_abertos.tb_estado.id_estado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_geracao', db.Float),
                extend_existing=True
            )

        elif table_name.lower() == 'tb_carga':
            
            table_schema = db.Table('tb_carga', self.meta,
                db.Column('id_subsistema', db.ForeignKey('db_ons.tb_submercado.cd_submercado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_carga_global', db.Float),
                db.Column('vl_carga_global_cons', db.Float),
                db.Column('vl_carga_global_smmgd', db.Float),
                db.Column('vl_carga_supervisionada', db.Float),
                db.Column('vl_carga_nao_supervisionada', db.Float),
                db.Column('vl_carga_mmgd', db.Float),
                db.Column('vl_consistencia', db.Float),
                extend_existing=True
            )
        elif table_name.lower() == 'tb_carga_programada':
            
            table_schema = db.Table('tb_carga_programada', self.meta,
                db.Column('id_subsistema', db.ForeignKey('db_ons.tb_submercado.cd_submercado'), primary_key=True),
                db.Column('dt_data_hora', db.DateTime, primary_key=True),
                db.Column('vl_carga_global_programada', db.Float),
                extend_existing=True
            )

        elif table_name.lower() == 'tb_chuva_obs_cpc':
            table_schema = db.Table('tb_chuva_obs_cpc', self.meta,
                db.Column('cd_subbacia', db.Integer),
                db.Column('dt_ini_observado', db.Date),
                db.Column('vl_chuva', db.Float)
            )
            
        elif table_name.lower() == 'tb_negociacoes_resumo':
            
            table_schema = db.Table('tb_negociacoes_resumo', self.meta,
                db.Column('id_produto', db.ForeignKey('bbce.tb_produtos.id_produto'), primary_key=True),
                db.Column('data', db.DateTime, primary_key=True),
                db.Column('preco_maximo', db.Float),
                db.Column('preco_minimo', db.Float),
                db.Column('preco_medio', db.Float),
                db.Column('preco_abertura', db.Float),
                db.Column('preco_fechamento', db.Float),
                db.Column('volume', db.Float),
                db.Column('total_negociacoes', db.Integer),            
                db.Column('hora_fechamento', db.Time),
                db.Column('id_categoria_negociacao', db.ForeignKey('bbce.tb_categoria_negociacao.id')),
                extend_existing=True
            )
            
        elif table_name.lower() == 'tb_pesos_grupos_ecmwf':
            table_schema = db.Table('tb_pesos_grupos_ecmwf', self.meta,
                db.Column('id_deck', db.String(100), nullable=True),
                db.Column('vl_peso_g1', db.Float, nullable=False),
                db.Column('vl_peso_g2', db.Float, nullable=False),
                db.Column('vl_peso_g3', db.Float, nullable=False),
                db.Column('vl_peso_g4', db.Float, nullable=False),
                db.Column('vl_peso_g5', db.Float, nullable=False),
                db.Column('vl_peso_g6', db.Float, nullable=False),
                db.Column('vl_peso_g7', db.Float, nullable=False),
                db.Column('vl_peso_g8', db.Float, nullable=False),
                db.Column('vl_peso_g9', db.Float, nullable=False),
                db.Column('vl_peso_g10', db.Float, nullable=False)
            )
        
        elif table_name.lower() == 'tb_geracao_horaria':
            table_schema = db.Table('tb_geracao_horaria', self.meta,
                db.Column('str_submercado', db.String(10), nullable=True),
                db.Column('dt_referente', db.DateTime, nullable=False),
                db.Column('vl_carga', db.Float, nullable=True),
                db.Column('cd_geracao', db.Integer, nullable=False),
                db.Column('dt_update', db.DateTime, nullable=True),
                db.Column('fl_excluir', db.Boolean, nullable=True)
            )

        elif table_name.lower() == 'tb_carga_horaria':
            table_schema = db.Table('tb_carga_horaria', self.meta,
                db.Column('str_submercado', db.String(22), nullable=True),
                db.Column('dt_referente', db.DateTime, nullable=False),
                db.Column('vl_carga', db.Float, nullable=True),
                db.Column('dt_update', db.DateTime, nullable=True),
                db.Column('fl_excluir', db.Boolean, nullable=True)
            )

        elif table_name.lower() == 'tb_regressoes':
            table_schema = db.Table('tb_regressoes', self.meta,
                db.Column('cd_posto_regredido', db.SmallInteger, nullable=False),
                db.Column('cd_posto_base', db.SmallInteger, nullable=False),
                db.Column('vl_mes', db.SmallInteger, nullable=False),
                db.Column('vl_A0', db.Float, nullable=False),
                db.Column('vl_A1', db.Float, nullable=False)
            )

        elif table_name.lower() == 'tb_mlt':
            table_schema = db.Table('tb_mlt', self.meta,
                db.Column('cd_submercado', db.Integer, nullable=False),
                db.Column('vl_mes', db.Integer, nullable=False),
                db.Column('vl_mlt', db.Float, nullable=False)
            )

        elif table_name.lower() == 'tb_tipo_geracao':

            table_schema = db.Table('tb_tipo_geracao', self.meta,
                db.Column('cd_geracao', db.Integer, nullable=False),
                db.Column('str_geracao', db.String(20), nullable=True)
            )

        elif table_name.lower() == 'tb_produtibilidade':

            table_schema = db.Table('tb_produtibilidade', self.meta,
                db.Column('cd_posto', db.SmallInteger, nullable=False),
                db.Column('str_posto', db.String(50), nullable=True),
                db.Column('vl_produtibilidade', db.Float, nullable=True),
                db.Column('cd_submercado', db.SmallInteger, nullable=True),
                db.Column('str_submercado', db.String(10), nullable=True),
                db.Column('str_sigla', db.String(2), nullable=True),
                db.Column('cd_bacia', db.SmallInteger, nullable=True),
                db.Column('str_bacia', db.String(25), nullable=True),
                db.Column('cd_ree', db.SmallInteger, nullable=True),
                db.Column('str_ree', db.String(25), nullable=True)
            )

        elif table_name.lower() == 'tb_fsarh':

            table_schema = db.Table('tb_fsarh', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('modified', db.DateTime, nullable=False),
                db.Column('temporalidade_restricao', db.String(255), nullable=True),
                db.Column('agente', db.String(255), nullable=True),
                db.Column('bacia', db.String(255), nullable=True),
                db.Column('rio', db.String(255), nullable=True),
                db.Column('reservatorio_relacionado', db.String(255), nullable=True),
                db.Column('restricoes_montante', db.String(255), nullable=True),
                db.Column('restricoes_jusante', db.String(255), nullable=True),
                db.Column('caracteristica_restricao', db.String(255), nullable=True),
                db.Column('classe_restricao', db.String(255), nullable=True),
                db.Column('novo_valor', db.Float, nullable=True),
                db.Column('valor_vigente', db.Float, nullable=True),
                db.Column('wfaprov', db.String(1024), nullable=True),
                db.Column('evento', db.String(1024), nullable=True),
                db.Column('justificativa', db.Text, nullable=True),
                db.Column('status', db.String(255), nullable=True),
                db.Column('reperm', db.String(1024), nullable=True),
                db.Column('caracteristica_restricao_ponto_cont', db.Text, nullable=True),
                db.Column('data_fim', db.DateTime, nullable=True),
                db.Column('data_inic', db.DateTime, nullable=True)
            )
        elif table_name.lower() == 'tb_intervencoes':

            table_schema = db.Table('tb_intervencoes', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('num_interv', db.String(255), nullable=True),
                db.Column('estado_interv', db.String(255), nullable=True),
                db.Column('nome_equipamento', db.String(255), nullable=True),
                db.Column('orgao_solic', db.String(255), nullable=True),
                db.Column('data_inicio', db.DateTime, nullable=False),
                db.Column('data_fim', db.DateTime, nullable=False),
                db.Column('periodo', db.String(255), nullable=True),
                db.Column('nome_categ', db.String(255), nullable=True),
                db.Column('tipo', db.Integer, nullable=False),
                db.Column('nome_natureza', db.String(255), nullable=True),
                db.Column('risco_postergacao', db.String(255), nullable=True),
                db.Column('recomendacao', db.Text, nullable=True)
            )
        elif table_name.lower() == 'tb_membro_modelo':
            table_schema = db.Table('tb_membro_modelo', self.meta,
                db.Column('id', db.INTEGER, primary_key=True),
                db.Column('dt_hr_rodada', db.DateTime, index=True),
                db.Column('nome', db.String(100)),
                db.Column('modelo', db.String(100)),
                db.Column('peso', db.Float),
                
            )
        
        elif table_name.lower() == 'tb_chuva_membro':
            table_schema = db.Table('tb_chuva_membro', self.meta,
                db.Column('id_membro_modelo', db.ForeignKey('db_rodadas.tb_membro_modelo.id'), primary_key=True),
                db.Column('cd_subbacia', db.ForeignKey('db_rodadas.tb_subbacia.cd_subbacia'), primary_key=True),
                db.Column('dt_prevista', db.Date, primary_key=True),
                db.Column('vl_chuva', db.Float),
            )

        elif table_name.lower() == 'tb_cadastro_estacao_chuvosa':

            table_schema = db.Table('tb_cadastro_estacao_chuvosa', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('dt_rodada', db.Date, nullable=True),
                db.Column('hr_rodada', db.Integer, nullable=True),
                db.Column('str_modelo', db.String(255), nullable=True),
            )


        elif table_name.lower() == 'tb_cadastro_estacao_chuvosa_norte':

            table_schema = db.Table('tb_cadastro_estacao_chuvosa_norte', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('dt_rodada', db.Date, nullable=True),
                db.Column('hr_rodada', db.Integer, nullable=True),
                db.Column('str_modelo', db.String(255), nullable=True),
            ) 


        elif table_name.lower() == 'tb_chuva_prevista_estacao_chuvosa':

            table_schema = db.Table('tb_chuva_prevista_estacao_chuvosa', self.meta,
                db.Column('id_cadastro', db.Integer, primary_key=True),
                db.Column('dt_prevista', db.Date, nullable=True),
                db.Column('vl_chuva', db.String(255), nullable=True),

            )

        elif table_name.lower() == 'tb_chuva_prevista_estacao_chuvosa_norte':

            table_schema = db.Table('tb_chuva_prevista_estacao_chuvosa_norte', self.meta,
                db.Column('id_cadastro', db.Integer),
                db.Column('dt_prevista', db.DateTime, nullable=True),
                db.Column('vl_chuva', db.String(255), nullable=True),

            )

        elif table_name.lower() == 'tb_chuva_observada_estacao_chuvosa':

            table_schema = db.Table('tb_chuva_observada_estacao_chuvosa', self.meta,
                db.Column('dt_observada', db.Date, nullable=True),
                db.Column('vl_chuva', db.Float, nullable=True),

            )

        elif table_name.lower() == 'tb_chuva_observada_estacao_chuvosa_norte':

            table_schema = db.Table('tb_chuva_observada_estacao_chuvosa_norte', self.meta,
                db.Column('dt_observada', db.Date, nullable=True),
                db.Column('vl_chuva', db.Float, nullable=True),

            )

        elif table_name.lower() == 'tb_climatologia_bacias_merge':

            table_schema = db.Table('tb_climatologia_bacias_merge', self.meta,
                db.Column('str_bacia', db.Text, nullable=True),
                db.Column('time', db.Date, nullable=True),
                db.Column('climatologia', db.Float, nullable=True),

            )

        elif table_name.lower() == 'tb_cadastro_vento_previsto':

            table_schema = db.Table('tb_cadastro_vento_previsto', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('dt_rodada', db.Date, nullable=True),
                db.Column('hr_rodada', db.Integer, nullable=True),
                db.Column('str_modelo', db.String(255), nullable=True),
            ) 

        elif table_name.lower() == 'tb_valores_vento_previsto':

            table_schema = db.Table('tb_valores_vento_previsto', self.meta,
                db.Column('id_cadastro', db.Integer),
                db.Column('dt_prevista', db.Date, nullable=True),
                db.Column('vl_vento', db.Float, nullable=True),
                db.Column('estado', db.String(255), nullable=True),
                db.Column('aglomerado', db.String(255), nullable=True),

            )

        elif table_name.lower() == 'tb_cmo_semanal':
            
            table_schema = db.Table('tb_cmo_semanal', self.meta,
                db.Column('id_subsistema', db.ForeignKey('db_ons.tb_submercado.cd_submercado'), primary_key=True),
                db.Column('dt', db.Date, primary_key=True),
                db.Column('vl_media_semanal', db.Float, nullable = True),
                db.Column('vl_cmo_leve', db.Float, nullable = True),
                db.Column('vl_cmo_media', db.Float, nullable = True),
                db.Column('vl_cmo_pesada', db.Float, nullable = True),
                extend_existing=True            
            )
            
        elif table_name.lower() == 'tb_produtos_interesse':
            table_schema = db.Table('tb_produtos_interesse', self.meta,
                db.Column('id_produto', db.ForeignKey('bbce.tb_produtos_interesse.id_produto'), primary_key=True),
                db.Column('ordem', db.Integer),
                extend_existing=True            
            )
        elif table_name.lower() == 'tb_categoria_negociacao':
            table_schema = db.Table('tb_categoria_negociacao', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('nome', db.String(20)),
                extend_existing=True
            )
        elif table_name.lower() == 'tb_dc_weol_semanal':
            table_schema = db.Table('tb_dc_weol_semanal', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('inicio_semana', db.Date),
                db.Column('final_semana', db.Date),
                db.Column('data_produto', db.Date),
                db.Column('submercado', db.String(2)),
                db.Column('patamar', db.String(6)),
                db.Column('valor', db.Float),
                db.Column('rv_atual', db.Integer),
                db.Column('mes_eletrico', db.Integer),
                extend_existing=True
            )
        elif table_name.lower() == 'tb_patamar_decomp':
            table_schema = db.Table('tb_patamar_decomp', self.meta,
                db.Column('inicio', db.DateTime),
                db.Column('patamar', db.String(6)),
                db.Column('cod_patamar', db.Integer),
                db.Column('dia_semana', db.String(3)),
                db.Column('dia_tipico', db.String(40)),
                db.Column('tipo_dia', db.String(3)),
                db.Column('intervalo', db.Integer),
                db.Column('dia', db.Integer),
                db.Column('semana', db.Integer),
                db.Column('mes', db.Integer),
                extend_existing=True
            )
        elif table_name.lower() == 'products':
            table_schema = db.Table('products', self.meta,
                db.Column('id', db.Integer, primary_key=True),
                db.Column('name', db.String(80), nullable=True),
                db.Column('last_received', db.String(80), nullable=True),
                db.Column('updated_at', db.DateTime, nullable=True),
                extend_existing=True
            )
            

        elif table_name.lower() == 'tb_usinas_termicas':
            table_schema = db.Table('tb_usinas_termicas', self.meta,
                db.Column("cd_usina",db.Integer, primary_key=True, nullable=False),
                db.Column("str_usina",db.String(100), nullable=True),
                db.Column("str_tipo",db.String(100), nullable=True),
                db.Column("flag_custo_fixo",db.Integer, nullable=False),
                extend_existing=True
            )
        elif table_name.lower() == 'tb_cvu':
            table_schema = db.Table('tb_cvu', self.meta,
                db.Column("cd_usina",db.Integer, db.ForeignKey('db_decks.tb_usinas_termicas.cd_usina'), primary_key=True, nullable=False),
                db.Column("vl_cvu",db.Float, nullable=False),
                db.Column("tipo_cvu",db.String(100), primary_key=True,nullable=False),
                db.Column("mes_referencia",db.String(100),primary_key=True, nullable=False),
                db.Column("ano_horizonte",db.Integer,primary_key=True, nullable=False),
                db.Column("dt_atualizacao",db.Date, primary_key=True,nullable=False),
                db.Column("fonte",db.String(100),primary_key=True, nullable=False),
                extend_existing=True
            )

        elif table_name.lower() == 'tb_cvu_merchant':
            table_schema = db.Table('tb_cvu_merchant', self.meta,
                db.Column("cd_usina",db.Integer,db.ForeignKey('db_decks.tb_usinas_termicas.cd_usina'), primary_key=True, nullable=False),
                db.Column("vl_cvu_cf",db.Float, nullable=False),
                db.Column("vl_cvu_scf",db.Float, nullable=False),
                db.Column("mes_referencia",db.String(100),primary_key=True, nullable=False),
                db.Column("dt_atualizacao",db.Date,primary_key=True, nullable=False),
                db.Column("fonte",db.String(100),primary_key=True, nullable=False),
                extend_existing=True
                
            )

        elif table_name.lower() == 'tb_re_limites':
            table_schema = db.Table('tb_restricao_eletrica', self.meta,
                db.Column("mes_referencia", db.String(6), primary_key=True, nullable=False),
                db.Column("cd_re", db.Integer, primary_key=True, nullable=False),
                db.Column("limite", db.String(100), nullable=True),
                extend_existing=True
                )
        
        elif table_name.lower() == 'tb_restricao_eletrica':
            table_schema = db.Table('tb_restricao_eletrica', self.meta,
                db.Column("mes_referencia", db.String(6), primary_key=True, nullable=False),
                db.Column("cd_re", db.Integer, primary_key=True, nullable=False),
                db.Column("limite", db.String(100), nullable=True),
                db.Column("primeiro_mes_pesada", db.Float, nullable=True),
                db.Column("primeiro_mes_media", db.Float, nullable=True),
                db.Column("primeiro_mes_leve", db.Float, nullable=True),
                db.Column("segundo_mes_pesada", db.Float, nullable=True),
                db.Column("segundo_mes_media", db.Float, nullable=True),
                db.Column("segundo_mes_leve", db.Float, nullable=True),
                extend_existing=True
            )
            
        elif table_name.lower() == 'ena_acomph':
            table_schema = db.Table('ena_acomph', self.meta,
                db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                db.Column('data', db.Date, nullable=False),
                db.Column('granularidade', db.String(20), nullable=False),
                db.Column('localizacao', db.String(50), nullable=True),
                db.Column('ena', db.Float, nullable=True),
                db.Index('idx_granularidade', 'granularidade'),
                db.Index('idx_data_granularidade', 'data', 'granularidade'),
                extend_existing=True
            )
        elif table_name.lower() == 'carga_semanal_dc':
            table_schema = db.Table('carga_semanal_dc', self.meta,
                db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                db.Column('data_produto', db.Date, nullable=True),
                db.Column('semana_operativa', db.Date, nullable=True),
                db.Column('patamar', db.String(10), nullable=True),
                db.Column('duracao', db.Float, nullable=True),
                db.Column('submercado', db.String(5), nullable=True),
                db.Column('carga', db.Numeric(10,2), nullable=True),
                db.Column('base_cgh', db.Numeric(10,4), nullable=True),
                db.Column('base_eol', db.Numeric(10,4), nullable=True),
                db.Column('base_ufv', db.Numeric(12,7), nullable=True),
                db.Column('base_ute', db.Numeric(10,4), nullable=True),
                db.Column('carga_mmgd', db.Float, nullable=True),
                db.Column('exp_cgh', db.Numeric(10,4), nullable=True),
                db.Column('exp_eol', db.Numeric(10,4), nullable=True),
                db.Column('exp_ufv', db.Numeric(12,6), nullable=True),
                db.Column('exp_ute', db.Numeric(10,4), nullable=True),
                db.Column('estagio', db.SmallInteger, nullable=True),
                extend_existing=True
            )
            
        elif table_name.lower() == 'acomph_consolidado':
            table_schema = db.Table('acomph_consolidado', self.meta,
                db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                db.Column('dt_referente', db.Date, nullable=True),
                db.Column('cd_posto', db.SmallInteger, nullable=True),
                db.Column('vl_vaz_def_conso', db.Float(12,6), nullable=True),
                db.Column('vl_vaz_inc_conso', db.Float(12,6), nullable=True),
                db.Column('vl_vaz_nat_conso', db.Float(12,6), nullable=True),
                db.Column('dt_acomph', db.Date, nullable=True),
                extend_existing=True
            )
        elif table_name.lower() == 'tb_vazoes_obs':
            table_schema = db.Table('tb_vazoes_obs', self.meta,
                    db.Column('txt_subbacia', db.String(100)),
                    db.Column('cd_estacao', db.Integer),
                    db.Column('txt_tipo_vaz', db.String(100)),
                    db.Column('dt_referente', db.DateTime),
                    db.Column('vl_vaz', db.Float)
                )
        elif table_name.lower() == 'carga_consolidada_pmo':
            table_schema = db.Table('carga_consolidada_pmo', self.meta,
                    db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                    db.Column('carga', db.Numeric(10, 2), nullable=False),
                    db.Column('mes', db.Integer, nullable=False),
                    db.Column('revisao', db.String(10), nullable=False),
                    db.Column('subsistema', db.String(4), nullable=False),
                    db.Column('semana', db.Integer),
                    db.Column('dt_inicio', db.Date, nullable=False),
                    db.Column('tipo', db.String(7), nullable=False),
                    db.Column('periodicidade_inicial', db.DateTime),
                    db.Column('periodicidade_final', db.DateTime),
                    db.Column('created_at', db.DateTime, default=db.func.current_timestamp()),
                    db.Column('updated_at', db.DateTime, default=db.func.current_timestamp(), 
                             onupdate=db.func.current_timestamp())
                )
        elif table_name.lower() == 'check_cvu':
            table_schema = db.Table('check_cvu', self.meta,
                db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                db.Column('tipo_csv', db.String(50), nullable=True),
                db.Column('data_atualizacao', db.DateTime, nullable=True),
                db.Column('status', db.String(50), nullable=True),
                db.Column('created_at', db.DateTime, default=db.func.current_timestamp()),
                db.Column('updated_at', db.DateTime, default=db.func.current_timestamp(), 
                         onupdate=db.func.current_timestamp())
            )
        return table_schema

            
if __name__ == '__main__':
    
    pass