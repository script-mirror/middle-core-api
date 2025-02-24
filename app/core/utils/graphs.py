import os
import requests as r
from app.core.config import settings

cognito_url = settings.cognito_url
cognito_config = settings.cognito_config

def get_color(modelo:str):
    cores = {
            'RV': '#6a5acd',
            'ACOMPH': '#06bbc7',
            'PCONJUNTO': 'rgba(0,0,255,1.0)',
            'GEFS': 'rgba(46,139,87,1.0)',
            'GFS': 'rgba(218,165,32,1.0)',
            'PZERADA': 'rgba(242,9,9,1.0)',
            'EC-ENSREMVIES': 'rgba(0,174,0,1.0)',
            'ETA': 'rgba(255,0,255,1.0)',
            'ETA40REMVIES': 'rgba(213,0,255,1.0)',
            'PCONJUNTO-EXT': 'rgba(129,214,0,1)',
            'GEFSREMVIES': 'rgba(224,107,11.0)',
            'PMEDIA': 'rgba(52, 56, 55, 1)',
            'PCONJUNTO2': 'rgba(93, 33, 208, 1)',
            'MLT': 'rgba(14,0,0)',
            'MERGE': 'rgba(94, 146, 206, 1)'
        }
    
    return cores.get(modelo, "#FF4444")

def get_access_token() -> str:
    response = r.post(
        cognito_url,
        data=cognito_config,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
 