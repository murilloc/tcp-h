from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela supplier
pg_cursor.execute("""
SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment
FROM public.supplier
""")
suppliers = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_supplier_to_redis(supplier):
    s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment = supplier

    # Chave do Redis
    redis_key = f'supplier:{s_suppkey}'

    # Verifica se a chave já existe e o tipo dela
    if redis_conn.exists(redis_key):
        if redis_conn.type(redis_key) != b'hash':
            raise ValueError(f"Chave {redis_key} já existe e não é do tipo hash.")

    # Dados do fornecedor como hash
    redis_conn.hset(redis_key, mapping={
        's_suppkey': s_suppkey,
        's_name': s_name.strip(),
        's_address': s_address,
        's_nationkey': s_nationkey,
        's_phone': s_phone,
        's_acctbal': str(s_acctbal),
        's_comment': s_comment
    })

    # Adicionando o fornecedor ao índice de nationkey
    redis_conn.sadd(f'nation:{s_nationkey}:suppliers', s_suppkey)

# Carregando todos os fornecedores no Redis
for supplier in suppliers:
    load_supplier_to_redis(supplier)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
