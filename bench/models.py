from django.db import models
from clickhouse_backend import models as ch


class Order(ch.ClickhouseModel):
    num = ch.StringField()
    
    class Meta:
        ordering = ['id']
        engine = ch.MergeTree(
            order_by=['id']
        )


class OrderComment(ch.ClickhouseModel):
    order = models.ForeignKey(Order, on_delete=models.CASCADE)
    text = ch.StringField()
    categories = ch.ArrayField(ch.StringField(low_cardinality=True))
    
    class Meta:
        ordering = ['id']
        engine = ch.MergeTree(
            order_by=['id']
        )
