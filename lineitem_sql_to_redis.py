from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela lineitem
pg_cursor.execute("""
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, 
       l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, 
       l_receiptdate, l_shipinstruct, l_shipmode, l_comment 
FROM public.lineitem
""")
lineitems = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_lineitem_to_redis(lineitem):
    (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
     l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate,
     l_receiptdate, l_shipinstruct, l_shipmode, l_comment) = lineitem

    # Chave do Redis
    redis_key = f'lineitem:{l_orderkey}:{l_linenumber}'

    # Dados do item da linha como hash
    redis_conn.hset(redis_key, mapping={
        'l_orderkey': l_orderkey,
        'l_partkey': l_partkey,
        'l_suppkey': l_suppkey,
        'l_linenumber': l_linenumber,
        'l_quantity': str(l_quantity),
        'l_extendedprice': str(l_extendedprice),
        'l_discount': str(l_discount),
        'l_tax': str(l_tax),
        'l_returnflag': l_returnflag,
        'l_linestatus': l_linestatus,
        'l_shipdate': l_shipdate.strftime('%Y-%m-%d %H:%M:%S') if l_shipdate else '',
        'l_commitdate': l_commitdate.strftime('%Y-%m-%d %H:%M:%S') if l_commitdate else '',
        'l_receiptdate': l_receiptdate.strftime('%Y-%m-%d %H:%M:%S') if l_receiptdate else '',
        'l_shipinstruct': l_shipinstruct,
        'l_shipmode': l_shipmode,
        'l_comment': l_comment
    })

# Carregando todos os itens da linha no Redis
for lineitem in lineitems:
    load_lineitem_to_redis(lineitem)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
