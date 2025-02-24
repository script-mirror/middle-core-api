import os
import requests
import pdb
import datetime
from typing import Optional
from app.core.config import settings

__url_airflow_api = settings.url_airflow_api 
__user_airflow = settings.user_airflow  
__password_airflow = settings.password_airflow 

def get_airflow_auth():
    return requests.auth.HTTPBasicAuth(__user_airflow, __password_airflow)

def trigger_airflow_dag(dag_id:str,json_produtos:dict={}, momento_req:Optional[datetime.datetime]=None):

    trigger_dag_url = f"{__url_airflow_api}/dags/{dag_id}/dagRuns"
    json = {"conf": json_produtos}
    if momento_req != None:
        json["dag_run_id"]=f"{json_produtos['modelos'][0][0]}{momento_req.strftime('_%d_%m_%Y_%H_%M_%S')}"

    # Faz a chamada para a API do Airflow com autenticação
    answer = requests.post(trigger_dag_url, json=json, auth=get_airflow_auth(),verify=False)
    return answer
    
def get_dag_run_state(dag_id:str, dag_run_id:str):
    get_dag_url = f"{__url_airflow_api}/dags/{dag_id}/dagRuns/{dag_run_id}"

    # Faz a chamada para a API do Airflow com autenticação
    answer = requests.get(get_dag_url, auth=get_airflow_auth(),verify=False)
    return {"state":answer.json()['state'], "end_datetime":datetime.datetime.strptime(answer.json()['end_date'][0:19] , '%Y-%m-%dT%H:%M:%S')}

def convert_modelos_to_json(dt_rodada:datetime.datetime,modelos_names:list=[], rodada:int=None):
    rodadas = {rodada:modelos_names}

    modelos = []
    for hr in rodadas:
        for modelo in rodadas[hr]:
            modelos += (modelo,hr,dt_rodada.strftime("%d/%m/%Y")),
    json_produtos = {"modelos": modelos}
    return json_produtos

def trigger_dag_smap(dt_rodada:datetime.date, modelos_names:list=[], rodada:int=None, momento_req:Optional[datetime.datetime]=None):

    json_produtos = convert_modelos_to_json(
        dt_rodada=dt_rodada,
        modelos_names=modelos_names,
        rodada=rodada)
    trigger_airflow_dag(dag_id='SSH_SMAP',json_produtos=json_produtos, momento_req=momento_req)
