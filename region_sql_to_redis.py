from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

pg_cursor.execute("""
SELECT r_regionkey, r_name, r_comment
FROM public.region
""")
regions = pg_cursor.fetchall()

# Função para carregar dados no Redis
def load_region_to_redis(region):
    r_regionkey, r_name, r_comment = region

    # Chave do Redis
    redis_key = f'region:{r_regionkey}'

    # Verifica se a chave já existe e o tipo dela
    if redis_conn.exists(redis_key):
        if redis_conn.type(redis_key) != b'hash':
            raise ValueError(f"Chave {redis_key} já existe e não é do tipo hash.")

    # Dados da região como hash
    redis_conn.hset(redis_key, mapping={
        'r_regionkey': r_regionkey,
        'r_name': r_name.strip(),
        'r_comment': r_comment
    })

# Carregando todas as regiões no Redis
for region in regions:
    load_region_to_redis(region)

# Fechando conexões
pg_cursor.close()
pg_conn.close()