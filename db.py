import redis

class RedisHandler(object):
    def __init__(self,):
        self.conn = redis.Redis('localhost')

    def hmset_insert(self,key,value):
        self.conn.hmset(key, value)

    def hmgetall_get(self,key):
        return self.conn.hgetall(key)

    def get_all_keys(self,query="*"):
        return self.conn.keys(query)

    def load_bulk(self,data):
        for item in data:
            key = item["SC_NAME"]
            self.hmset_insert(key,item)
