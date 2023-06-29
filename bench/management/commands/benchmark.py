from bench.models import Order, OrderComment
from bench.generic.aggregates import GroupArray

from django.core.management.base import BaseCommand
from django.db import connections
from django.db.models.query import Prefetch
from notalib.time import Timing


class Section(Timing):
    def __init__(self, label: str = ''):
        super().__init__(True)
        self.label = label
    
    def __enter__(self):
        print('\n' + self.label)
        print('-' * len(self.label))
        return super().__enter__()


class Command(BaseCommand):
    help = "Closes the specified poll for voting"
    divider = '\n============================'
    connection = connections['clickhouse']

    def handle(self, *args, **options):
        self.pre(*args, **options)
        print(self.divider)
        self.stage1(*args, **options)
        print(self.divider)
        self.stage2(*args, **options)
        print(self.divider)
        self.stage3(*args, **options)
        print(self.divider)
        self.stage4(*args, **options)
        print(self.divider)
        self.stage5(*args, **options)
        print(self.divider)
        self.stage6(*args, **options)
        print(self.divider)
        self.stage7(*args, **options)
        print(self.divider)
        self.stage8(*args, **options)
        print(self.divider)
        self.stage9(*args, **options)
        print(self.divider)

    def pre(*args, **options):
        print(f"Orders count:", Order.objects.all().count())
        print(f"OrderComments count:", OrderComment.objects.all().count())

    def stage1(self, *args, **options):
        print("Stage 1 - INNER JOIN")
        print("--------------------")
        
        with Section("Raw SQL"): 
            with self.connection.cursor() as c:
                c.execute("SELECT id, bench_order.num FROM bench_ordercomment INNER JOIN bench_order ON bench_ordercomment.order_id = bench_order.id")
                print(c.fetchone())
        
        with Section("Django ORM"):
            print(OrderComment.objects.all().select_related('order').values('id', 'order__num').first())

    def stage2(self, *args, **options):
        print("Stage 2 - LEFT OUTER JOIN")
        print("-------------------------")
        
        with Section("Raw SQL"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                LEFT OUTER JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())
                
        with Section("Django ORM [with annotate]"):
            print(Order.objects.all().annotate(texts=GroupArray('ordercomment__text')).values('id', 'texts')[0])
            
        with Section("Django ORM [with prefetch]"):
            order = Order.objects.all().prefetch_related(Prefetch('ordercomment_set'))[0]
            print({'id': order.id, 'texts': [c.text for c in order.ordercomment_set.all()]})

    def stage3(self, *args, **options):
        print("Stage 3 - RIGHT OUTER JOIN")
        print("--------------------------")
        
        with Section("Raw SQL"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                RIGHT OUTER JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())

    def stage4(self, *args, **options):
        print("Stage 4 - FULL OUTER JOIN")
        print("-------------------------")
        
        with Section("Raw SQL"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                FULL OUTER JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())

    def stage5(self, *args, **options):
        print("Stage 5 - CROSS JOIN")
        print("--------------------")
        
        print("Skipped. Reason: Consumes too many resources")

    def stage6(self, *args, **options):
        print("Stage 6 - SEMI JOIN")
        print("-------------------")
        
        with Section("Raw SQL [LEFT SEMI JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                LEFT SEMI JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())
                
        with Section("Raw SQL [RIGHT SEMI JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                RIGHT SEMI JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())

    def stage7(self, *args, **options):
        print("Stage 7 - ANTI JOIN")
        print("-------------------")
        
        with Section("Raw SQL [LEFT ANTI JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                LEFT ANTI JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())
                
        with Section("Raw SQL [RIGHT ANTI JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                RIGHT ANTI JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())

    def stage8(self, *args, **options):
        print("Stage 8 - ANY JOIN")
        print("------------------")
        
        with Section("Raw SQL [LEFT ANY JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                LEFT ANY JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())
                
        with Section("Raw SQL [RIGHT ANY JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                RIGHT ANY JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())
                
        with Section("Raw SQL [INNER ANY JOIN]"): 
            with self.connection.cursor() as c:
                c.execute("""
                SELECT id, groupArray(bench_ordercomment.text) as texts
                FROM bench_order
                INNER ANY JOIN bench_ordercomment ON bench_order.id = bench_ordercomment.order_id
                GROUP BY id, num
                """)
                print(c.fetchone())

    def stage9(self, *args, **options):
        print("Stage 9 - ASOF JOIN")
        print("-------------------")
        print("Skipper. Reason: Too lazy to prepare test tables with special columns")
