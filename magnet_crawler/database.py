import json
import logging
import sqlite3
import string
from datetime import datetime

import redis

# redis config
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
# 所有的magnet存在这里
REDIS_ALL_KEY = 'all-magnet'
# 进行过转换的magnet
REDIS_USED_KEY = 'used-magnet'
# 能下载的magnet
REDIS_AVAIL_KEY = 'magnet'

# mysql config
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306

# sqlite3
SQLITE_DATABASE_NAME = 'magnet.db'


class RedisClient:
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT):
        pool = redis.ConnectionPool(host=host, port=port, db=0)
        self.client = redis.Redis(connection_pool=pool)

    def add(self, magnet, key=REDIS_ALL_KEY):
        self.client.sadd(key, magnet)

    def count(self, key=REDIS_ALL_KEY):
        return self.client.scard(key)

    def get(self, count, keys=REDIS_ALL_KEY):
        """用于转换magnet"""
        # keys = (REDIS_ALL_KEY, REDIS_USED_KEY)
        diff_set = self.client.sdiff(keys)
        length = len(diff_set)
        count = count if length > count else length
        magnets = [diff_set.pop() for _ in range(count)]
        return magnets

    def diff(self, keys, count):
        diff_set = self.client.sdiff(keys)
        length = len(diff_set)
        count = count if length > count else length
        magnets = [diff_set.pop() for _ in range(count)]
        return magnets


class MysqlClient:
    pass


class SqliteClient:
    def __init__(self, db):
        self.db = db
        # sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread.
        self.conn = sqlite3.connect(db, check_same_thread=False)

    def insert(self, magnet, data):
        insert_sql = '''
                insert into {table_name}
                (magnet, torrent_name, content, create_date)
                values(?, ?, ?, ?);
                '''
        cursor = self.conn.cursor()
        table_name = 'magnet_{}'.format(magnet[-40].lower())
        # table_name = 'magnet_{}'.format(magnet[0])
        try:
            params = (magnet, data.get('name', None), json.dumps(data), datetime.now(),)
            cursor.execute(insert_sql.format(table_name=table_name), params)
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)
        finally:
            cursor.close()
            self.conn.commit()

    def count(self):
        pass


def create_tables(db):
    create_sql = '''
    create table {table_name}
    (
      id integer not null primary key autoincrement,
      magnet varchar(30) not null unique,
      torrent_name varchar(500),
      content text,
      create_date datetime(6)
    );
    '''
    drop_sql = '''
    drop table {table_name};
    '''
    table_names = ['magnet_' + i for i in string.digits + string.ascii_lowercase]
    exec_tables = []

    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    try:
        for name in table_names:
            try:
                cursor.execute(create_sql.format(table_name=name))
                exec_tables.append(name)
                conn.commit()
                print('table {} created successful'.format(name))
            except sqlite3.OperationalError as e:
                # logging.exception(e)
                if 'already exists' in str(e):
                    print(e)
                    continue
                else:
                    raise Exception
    except sqlite3.OperationalError as e:
        logging.exception(e)
    except KeyboardInterrupt:
        for name in exec_tables:
            cursor.execute(drop_sql.format(table_name=name))
            conn.commit()
    finally:
        cursor.close()
        conn.commit()
        conn.close()


if __name__ == '__main__':
    # rc = RedisClient()
    # magnets = rc.get(20)
    # print(magnets)
    # print(len(magnets))
    create_tables('magnet.db')

