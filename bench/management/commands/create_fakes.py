from bench.models import Order, OrderComment

from random import choices, choice, randint

from django.core.management.base import BaseCommand
from faker import Faker


FAKER = Faker()
CATEGORIES = ['Phil', 'Mark', 'Bobby']


class Command(BaseCommand):
    help = "Closes the specified poll for voting"

    def handle(self, *args, **options):
        order_num = 0
        
        for _ in range(10000):
            orders_to_create = []
            
            for _ in range(1000):
                orders_to_create.append(Order(num=str(order_num)))
                order_num += 1

            orders = Order.objects.bulk_create(orders_to_create)
            print("Orders have been created")
            orders_with_comments = choices(orders, k=randint(10, len(orders)))
            comments_to_create = []
            
            for _ in range(randint(len(orders_with_comments), len(orders))):
                order = choice(orders_with_comments)
                comments_to_create.append(
                    OrderComment(order_id=order.id, text=FAKER.text(), categories=choices(CATEGORIES, k=randint(1, 2)))
                )
            
            OrderComment.objects.bulk_create(comments_to_create)
            print("Comments have been created")
