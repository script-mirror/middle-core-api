from sys import path

import diskcache as dc
from typing import Callable

cache = dc.Cache(f'{path[0]}/cache')

def get_key(function_key, *args):
    key = str(args).replace('(', '').replace(')', '').replace(',', ':').replace('\'', '')
    return f'{function_key}:{key}'

def get_cached(function:Callable, *args, atualizar:bool = False, timeout=60*60*24*7) -> dict:
    key = get_key(function.__name__, *args)
    if not atualizar:
        cached = cache.get(key)
        if cached:
            return cached
        data = function(*args)
        cache.set(key, data, expire=timeout)
        return data
    data = function(*args)
    cache.set(key, data, expire=timeout)
    return data
