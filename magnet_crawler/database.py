import redis

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_KEY = 'magnet'

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306


class RedisClient:
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, key=REDIS_KEY):
        pool = redis.ConnectionPool(host=host, port=port, db=0)
        self.client = redis.Redis(connection_pool=pool)
        self.key = key

    def add(self, magnet):
        self.client.sadd(self.key, magnet)

    def count(self):
        return self.client.scard(self.key)


class MysqlClient:
    pass
