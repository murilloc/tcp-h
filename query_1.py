from database_connections import connect_to_postgres, connect_to_redis
from datetime import datetime
from collections import defaultdict

# Conexão com o PostgreSQL
pg_conn, pg_cursor = connect_to_postgres()

# Conexão com o Redis
redis_conn = connect_to_redis()

# Parâmetros da consulta
target_date = datetime.strptime('1998-12-01', '%Y-%m-%d')

# Estrutura para armazenar os resultados agregados
results = defaultdict(lambda: {
    'sum_qty': 0,
    'sum_base_price': 0,
    'sum_disc_price': 0,
    'sum_charge': 0,
    'sum_discount': 0,
    'sum_extendedprice': 0,
    'count_order': 0
})

# Função para converter string para float de forma segura
def to_float(value):
    try:
        return float(value)
    except ValueError:
        return 0.0

# Percorrendo os dados da tabela lineitem
keys = redis_conn.keys('lineitem:*')
for key in keys:
    lineitem = redis_conn.hgetall(key)

    # Filtro de data
    l_shipdate = datetime.strptime(lineitem[b'l_shipdate'].decode('utf-8'), '%Y-%m-%d %H:%M:%S')
    if l_shipdate > target_date:
        continue

    l_returnflag = lineitem[b'l_returnflag'].decode('utf-8')
    l_linestatus = lineitem[b'l_linestatus'].decode('utf-8')

    l_quantity = to_float(lineitem[b'l_quantity'])
    l_extendedprice = to_float(lineitem[b'l_extendedprice'])
    l_discount = to_float(lineitem[b'l_discount'])
    l_tax = to_float(lineitem[b'l_tax'])

    key = (l_returnflag, l_linestatus)

    results[key]['sum_qty'] += l_quantity
    results[key]['sum_base_price'] += l_extendedprice
    results[key]['sum_disc_price'] += l_extendedprice * (1 - l_discount)
    results[key]['sum_charge'] += l_extendedprice * (1 - l_discount) * (1 + l_tax)
    results[key]['sum_discount'] += l_discount
    results[key]['sum_extendedprice'] += l_extendedprice
    results[key]['count_order'] += 1

# Calculando as médias
for key, value in results.items():
    count_order = value['count_order']
    value['avg_qty'] = value['sum_qty'] / count_order if count_order > 0 else 0
    value['avg_price'] = value['sum_extendedprice'] / count_order if count_order > 0 else 0
    value['avg_disc'] = value['sum_discount'] / count_order if count_order > 0 else 0

# Ordenando os resultados
sorted_results = sorted(results.items(), key=lambda x: (x[0][0], x[0][1]))

# Imprimindo os resultados
print(f"{'l_returnflag':<15} {'l_linestatus':<15} {'sum_qty':<15} {'sum_base_price':<20} {'sum_disc_price':<20} {'sum_charge':<20} {'avg_qty':<15} {'avg_price':<15} {'avg_disc':<15} {'count_order':<15}")
for (l_returnflag, l_linestatus), value in sorted_results:
    print(f"{l_returnflag:<15} {l_linestatus:<15} {value['sum_qty']:<15.2f} {value['sum_base_price']:<20.2f} {value['sum_disc_price']:<20.2f} {value['sum_charge']:<20.2f} {value['avg_qty']:<15.2f} {value['avg_price']:<15.2f} {value['avg_disc']:<15.2f} {value['count_order']:<15}")
