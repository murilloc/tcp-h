from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela nation
pg_cursor.execute("""
SELECT n_nationkey, n_name, n_regionkey, n_comment
FROM public.nation
""")
nations = pg_cursor.fetchall()


# Função para carregar dados no Redis
def load_nation_to_redis(nation):
    n_nationkey, n_name, n_regionkey, n_comment = nation

    # Chave do Redis
    redis_key = f'nation:{n_nationkey}'

    # Dados da nação como hash
    redis_conn.hset(redis_key, mapping={
        'n_nationkey': n_nationkey,
        'n_name': n_name.strip(),
        'n_regionkey': n_regionkey,
        'n_comment': n_comment
    })

    # Adicionando a nação ao índice de regionkey
    redis_conn.sadd(f'region:{n_regionkey}:nations', n_nationkey)


# Carregando todas as nações no Redis
for nation in nations:
    load_nation_to_redis(nation)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
