from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela orders
pg_cursor.execute("""
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
       o_orderpriority, o_clerk, o_shippriority, o_comment
FROM public.orders
""")
orders = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_order_to_redis(order):
    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
     o_orderpriority, o_clerk, o_shippriority, o_comment) = order

    # Chave do Redis
    redis_key = f'order:{o_orderkey}'

    # Dados do pedido como hash
    redis_conn.hset(redis_key, mapping={
        'o_orderkey': o_orderkey,
        'o_custkey': o_custkey,
        'o_orderstatus': o_orderstatus,
        'o_totalprice': str(o_totalprice),
        'o_orderdate': o_orderdate.strftime('%Y-%m-%d %H:%M:%S') if o_orderdate else '',
        'o_orderpriority': o_orderpriority.strip(),
        'o_clerk': o_clerk.strip(),
        'o_shippriority': o_shippriority,
        'o_comment': o_comment
    })

    # Adicionando o pedido ao índice de customer
    redis_conn.sadd(f'customer:{o_custkey}:orders', o_orderkey)

# Carregando todos os pedidos no Redis
for order in orders:
    load_order_to_redis(order)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
