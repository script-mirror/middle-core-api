import datetime
import sqlalchemy as db
import pandas as pd
from app.sintegre.bot_sintegre import trigger_bot, get_products
from app.core.database.wx_dbClass import db_mysql_master
from .schema import *
from ..core.utils import text

__DB__: db_mysql_master = db_mysql_master('bot_sintegre')


class Product:
    tb = __DB__.getSchema('products')

    @staticmethod
    def trigger(product_date: datetime.date, trigger_webhook: bool):
        return trigger_bot(product_date, trigger_webhook=trigger_webhook)

    @staticmethod
    def trigger_by_id(product_date: datetime.date, product_id: int):
        return trigger_bot(product_date, product_id)

    @staticmethod
    def get_products_json():

        return get_products(None)

    @staticmethod
    def get_product_by_id(product_id: int):
        return get_products(product_id)

    @staticmethod
    def get_products():
        query = db.select(
            Product.tb.c['id'],
            Product.tb.c['name'],
            Product.tb.c['last_received'],
            Product.tb.c['updated_at']
        )
        result = Product.__DB__.db_execute(query, commit=True).fetchall()
        df = pd.DataFrame(
            result, columns=['id', 'name', 'last_received', 'updated_at'])
        return df.to_dict('records')

    @staticmethod
    def update_product(product_id: int, last_received: str):
        query = db.update(Product.tb).values({'updated_at': datetime.datetime.now(
        ), 'last_received': last_received}).where(Product.tb.c['id'] == product_id)
        Product.__DB__.db_execute(query, commit=True)
        return True

    @staticmethod
    def verify_if_is_new(product_details: ProductUpdate):
        product_details = product_details.dict()
        products = Product.get_products()
        product = [x for x in products if x['name'] ==
                   text.remover_acentos_caracteres_especiais(product_details['nome'])][0]
        if product['last_received'] == product_details['fileHash']:
            return False
        Product.update_product(product['id'], product_details['fileHash'])
        return True
