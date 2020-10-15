
'''
@date:19-5-7
@desc:
1)redis的连接及工具
'''

import redis as pyredis

proxy_ua_redis = pyredis.StrictRedis(
    host = '192.168.251.4',
    port=5201,
    db=4,
    password='oio'
)
url_hash_redis = pyredis.StrictRedis(
    #host = '192.168.251.4',
    host = '192.168.186.184',
    port=5201,
    db=5,
    password='oio'
)
