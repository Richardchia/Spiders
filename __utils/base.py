
'''
@date:19-5-7
@desc:
1)爬虫基本依赖
'''

import time
import random
import datetime
from simhash import Simhash

from __dber.redis_client import proxy_ua_redis
from __dber.redis_client import url_hash_redis

def get_proxy_rest():
    '''REST API取proxy
    一般来自第三方proxy
    '''
    pass
def get_proxy_redis():
    '''取redis proxy
    自建池
    return:'ip:port'
    '''
    plen = proxy_ua_redis.llen('hw_proxy')
    return proxy_ua_redis.lindex("hw_proxy",random.choice(range(0,plen)))
def get_useragent():
    '''
    redis user agent pool
    return: str
    '''
    plen = proxy_ua_redis.llen('user_agent')
    return proxy_ua_redis.lindex("user_agent",random.choice(range(0,plen)))

def get_header():
    '''
    自动构建header
    '''
    header ={
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"en-US,en;q=0.9",
            "Connection":"keep-alive",
            "Upgrade-Insecure-Requests":"1",
            # "User-Agent":'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
            "User-Agent":get_useragent()
            }    
    return header

def has_url_crawled(url=None,method='hash'):
    '''
    if method=='bloomf':
        采用redis实现bloom-filter，判断url是否已爬取
    elif method==hash:
        将url的hash写入redis，用redis的集合去重
    url: str
    return: Boolean, True if url 已被爬过 else False
    '''
    if method=='hash':
        uh = Simhash(url).value
        return url_hash_redis.sismember('url_hash',uh)
    else:
        raise

def url_to_bloomf(url=None, expire=None):
    '''
    已爬url写入bloom-filter
    url: str
    expire:有效保存时间（便于某些url更新爬取），目前redis list 或 set结构下单个元素的过期不能做到，
    参考https://blog.csdn.net/leean950806/article/details/78669070
    '''
def url_to_hash(url=None, expire=None):
    '''
    已爬url写入的hash写入redis
    url: str
    expire:有效保存时间（便于某些url更新爬取），目前redis list 或 set结构下单个元素的过期不能做到，
    参考https://blog.csdn.net/leean950806/article/details/78669070
    后续可考虑将url hash set 划分为按天的小集合
    '''
    uh = Simhash(url).value
    url_hash_redis.sadd('url_hash',uh)

def str_simhash(txt):
    '''
    import: str
    return: simhash值
    '''
    return Simhash(txt).value

def get_now_str():
    '''
    取得当前时间字符串：like："2019-05-07 15:04:22.098756"
    '''
    dt = datetime.datetime.utcfromtimestamp(time.time()) + datetime.timedelta(hours=8)
    return str(dt)

def get_date_str():
    return datetime.datetime.now().strftime('%Y-%m-%d')

if __name__ == '__main__':
    pass
    
