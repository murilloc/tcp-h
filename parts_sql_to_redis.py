from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela part
pg_cursor.execute("""
SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment
FROM public.part
""")
parts = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_part_to_redis(part):
    p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment = part

    # Chave do Redis
    redis_key = f'part:{p_partkey}'

    # Dados da peça como hash
    redis_conn.hset(redis_key, mapping={
        'p_partkey': p_partkey,
        'p_name': p_name,
        'p_mfgr': p_mfgr.strip(),
        'p_brand': p_brand.strip(),
        'p_type': p_type,
        'p_size': p_size,
        'p_container': p_container.strip(),
        'p_retailprice': str(p_retailprice),
        'p_comment': p_comment
    })

# Carregando todas as peças no Redis
for part in parts:
    load_part_to_redis(part)

# Fechando conexões
pg_cursor.close()
pg_conn.close()

