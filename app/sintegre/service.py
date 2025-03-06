import datetime
import sqlalchemy as db
import pandas as pd
from app.sintegre.bot_sintegre import trigger_bot, get_products
from app.core.database.wx_dbClass import db_mysql_master
from .schema import *
from ..core.utils import text

class ProductService:
    __DB__:db_mysql_master
    tb:db.Table    
    def __init__(self):
        self.__DB__ = db_mysql_master('bot_sintegre')
        self.tb = self.__DB__.getSchema('products')
    def trigger(self, product_date:datetime.date, trigger_webhook:bool):
        return trigger_bot(product_date, trigger_webhook=trigger_webhook)

    def trigger_by_id(self, product_date: datetime.date, product_id: int):
        return trigger_bot(product_date, product_id)

    def get_products_json(self):
        return get_products(None)

    def get_product_by_id(self, product_id: int):
        return get_products(product_id)
    
    def get_products(self):
        query = db.select(
            self.tb.c['id'],
            self.tb.c['name'],
            self.tb.c['last_received'],
            self.tb.c['updated_at']
        )
        result = self.__DB__.db_execute(query, commit=True).fetchall()
        df = pd.DataFrame(result, columns=['id', 'name', 'last_received', 'updated_at'])
        return df.to_dict('records')
        
    def update_product(self, product_id: int, last_received: str):
        query = db.update(self.tb).values({'updated_at':datetime.datetime.now(), 'last_received':last_received}).where(self.tb.c['id'] == product_id)
        self.__DB__.db_execute(query, commit=True)
        return True
    
    def verify_if_is_new(self, product_details:ProductUpdate):
        product_details = product_details.dict()
        products = self.get_products()
        product = [x for x in products if x['name'] == text.remover_acentos_caracteres_especiais(product_details['nome'])][0]
        if product['last_received'] == product_details['fileHash']:
            return False
        self.update_product(product['id'], product_details['fileHash'])
        return True
    
    