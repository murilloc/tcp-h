from database_connections import connect_to_postgres, connect_to_redis

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Query para selecionar dados da tabela customer
pg_cursor.execute(
    "SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment FROM public.customer")
customers = pg_cursor.fetchall()


# Função para carregar dados no Redis
def load_customer_to_redis(customer):
    c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment = customer

    # Chave do Redis
    redis_key = f'customer:{c_custkey}'

    # Dados do cliente como hash
    redis_conn.hset(redis_key, mapping={
        'c_custkey': c_custkey,
        'c_name': c_name,
        'c_address': c_address,
        'c_nationkey': c_nationkey,
        'c_phone': c_phone,
        'c_acctbal': str(c_acctbal),  # Convertendo para string para evitar problemas com precisão
        'c_mktsegment': c_mktsegment,
        'c_comment': c_comment
    })

    # Adicionando o cliente ao índice de nationkey
    redis_conn.sadd(f'nation:{c_nationkey}:customers', c_custkey)


# Carregando todos os clientes no Redis
for customer in customers:
    load_customer_to_redis(customer)

# Fechando conexões
pg_cursor.close()
pg_conn.close()
