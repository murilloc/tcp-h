import psycopg2
import redis

def connect_to_postgres():
    pg_conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='changeme!',
        host='localhost',
        port='5432'
    )
    pg_cursor = pg_conn.cursor()
    return pg_conn, pg_cursor

def connect_to_redis():
    redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0)
    return redis_conn