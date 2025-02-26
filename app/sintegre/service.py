import datetime
from app.sintegre.bot_sintegre import trigger_bot, get_products

from app.core.database.wx_dbClass import db_mysql_master
__DB__ = db_mysql_master('sintegre')

class ProductService:
    def __init__(self):
        pass
    def trigger(self, product_date:datetime.date):
        return trigger_bot(product_date)

    def trigger_by_id(self, product_date: datetime.date, product_id: int):
        return trigger_bot(product_date, product_id)

    def get_products(self):
        return get_products(None)

    def get_product_by_id(self, product_id: int):
        return get_products(product_id)