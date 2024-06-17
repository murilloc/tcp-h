from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela partsupp
pg_cursor.execute("""
SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
FROM public.partsupp
""")
partsupps = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_partsupp_to_redis(partsupp):
    ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment = partsupp

    # Chave do Redis
    redis_key = f'partsupp:{ps_partkey}:{ps_suppkey}'

    # Dados do fornecedor de peça como hash
    redis_conn.hset(redis_key, mapping={
        'ps_partkey': ps_partkey,
        'ps_suppkey': ps_suppkey,
        'ps_availqty': ps_availqty,
        'ps_supplycost': str(ps_supplycost),
        'ps_comment': ps_comment
    })

    # Adicionando o fornecedor da peça ao índice de partkey
    redis_conn.sadd(f'part:{ps_partkey}:suppliers', ps_suppkey)

# Carregando todos os fornecedores de peças no Redis
for partsupp in partsupps:
    load_partsupp_to_redis(partsupp)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
