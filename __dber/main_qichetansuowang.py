# -*- coding: utf-8 -*-
"""
Created on Wed May 29 11:04:59 2019

@author: gz02399
"""

import __init__
import requests 
import re
import time
import random
from mrq.task import Task
from mrq.job import queue_job
from mrq.context import log,traceback
from retrying import retry

from __dber.pg_client import session
from __dber.pg_orm_qichetansuowang import Jz_qichetansuowang_PageSource, Jz_qichetansuowang_content
from __utils.base import get_date_str,get_header,get_now_str,str_simhash,get_proxy_redis
sess=session()

class Start(Task):
    def run(self,params):
        #初始化的时候，选用页面长度为30
        #每日增量，只要一页就可以
        url_s1 = ('http://www.feelcars.com/category/xinnengyuan/page/%s','新能源')
        end = 4
        for i in range(1,end):
            url = url_s1[0]%str(i)
            log.info('入队列 jz_qckj_pagesource')
            queue_job('main_qichetansuowang.Crawler1',
                      {'url':url,'flag':url_s1[-1]},
                      queue = 'jz_qckj_pagesource')
                
class Crawler1(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        print(url)
        try:
            ps = crawl(url)
            if len(ps) >2000:
                uid = store2pg(ps = ps,url = url,flag = flag)
            else:
                uid = None
            if uid:
                urls = re.compile('<a target="_blank" href="(http://www.feelcars.com/.*?html)">',re.S).findall(ps)
                for u in urls:
                    url = u
                    log.info('入队列 jz_qckj_pagesource')
                    queue_job('main_qichetansuowang.Crawler2',
                              {'url':url,'flag':flag},
                              queue = 'jz_qckj_pagesource') 
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qckj_pagesource')
            queue_job('main_qichetansuowang.Crawler1',
                      {'url':url,'flag':flag},
                      queue = 'jz_qckj_pagesource')
            
class Crawler2(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess.query(Jz_qichetansuowang_PageSource).filter_by(url = url).first()
            sess.rollback()
            if not info:
                ps = crawl(url)
                if len(str(ps))>2000:
                    uid = store2pg(ps = ps,url = url,flag = flag)
                else:
                    uid = None
                if uid:
                    log.info('入队列 jz_qckj_parse')
                    queue_job('main_qichetansuowang.Parse',
                              {'url':url,'flag':flag},
                              queue = 'jz_qckj_parse')
            else:
                print('新闻已存在，并入解析')   
                queue_job('main_qichetansuowang.Parse',
                              {'url':url,'flag':flag},
                              queue = 'jz_qckj_parse')
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qckj_pagesource')
            queue_job('main_qichetansuowang.Crawler2',
                      {'url':url,'flag':flag},
                      queue = 'jz_qckj_pagesource') 
        
class Parse(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess.query(Jz_qichetansuowang_content).filter_by(url = url).first()
            sess.rollback()
            if not info:
                info_2 = sess.query(Jz_qichetansuowang_PageSource).filter_by(url = url).first()
                sess.rollback()
                ps = info_2.pagesource
                ps_uid = info_2.uid
                author = re.findall('<span>本文源自(.*?) <a',ps)
                author = author[0] if author else None
                public_time = re.compile('发布日期：(.*?)日',re.S).findall(ps)
                public_time = public_time[0].replace('年','').replace('月','').strip() if public_time else None
                content1 = re.compile('<div class="entry-content">.*?<div id="social">',re.S).findall(ps)
                content = content1[0].replace('\n','').replace('\r','').replace('\t','') if content1 else None 
                content = re.sub('<.*?>','',content.replace('&rdquo;','').replace('&ldquo;','')).replace('&mdash;','').strip()
                title = re.compile('<h1 class="entry-title">(.*?)</h1>',re.S).findall(ps)
                title = title[0].strip() if title else None
                pic = re.findall('<img src="(.*?)"',content1[0]) if content else []
                pic = ';'.join(pic)
#                tag = re.findall('<meta name="keywords" content="(.*?)">',ps)
#                tag = tag[0] if tag else None

                hid = store2pg_parse(url = url, author = author, public_time= public_time,
                                     page_source= ps_uid, content= content, 
                                     website_name= '汽车探索网', channel_name= flag, title= title,
                                     topic = None,tag = None, meta_keywords = None,
                                     pic = pic,flag = flag)
                if hid:
                    print('完成') 
            else:
                print('新闻解析已存在')  
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qckj_parse')
            queue_job('main_qichetansuowang.Parse',
                      {'url':url,'flag':flag},
                      queue = 'jz_qckj_parse')
    
def tran2pg(ps=None, url=None, flag=None):
    '''
    转换为ORM obj
    '''
    crawlt = get_now_str()
    crawld = get_date_str()
    _hid=str_simhash(crawlt+url)
    _obj = Jz_qichetansuowang_PageSource(uid=_hid,
                             url=url,
                             collection_time = crawlt,
                             pagesource=ps,
                             flag=flag,
                             write_date=crawld)
    return _hid, _obj
def store2pg(ps=None, url=None, flag=None):
    '''存到pg
    cnt: 源码
    url: url
    '''
    try:
        uid, _obj = tran2pg(ps=ps,url=url,flag=flag)
        sess = session()
        sess.add(_obj)
        sess.commit()
        return uid
    except Exception as e:
        sess.rollback()
        traceback.print_exc()
        return False
    finally:
        sess.close()     

def tran2pg_parse(url = None, author = None, public_time= None,
               page_source= None, content= None, 
               website_name= None, channel_name= None, title= None,
               topic = None,tag = None,meta_keywords = None,
               pic = None,flag = None):
    '''
    转换为ORM obj
    '''
    crawlt = get_now_str()
    crawld = get_date_str()
    hid=str_simhash(crawlt+url)
    obj = Jz_qichetansuowang_content( 
                url = url ,
                author = author ,
                public_time= public_time,
                collection_time= crawlt,
                page_source= str(page_source),
                content= content,
                website_name= website_name,
                channel_name= channel_name,
                title= title,
                topic = topic, 
                tag = tag,
                meta_keywords = meta_keywords,
                write_date=crawld,
                pic = pic,
                flag = flag
                )
    return hid, obj        
def store2pg_parse(url = None, author = None, public_time= None,
               page_source= None, content= None, 
               website_name= None, channel_name= None, title= None,
               topic = None,tag = None,meta_keywords = None,
               pic = None,flag = None):
    '''存到pg
    cnt: 源码
    url: url
    '''
    try:
        sess=session()
        uid, _obj = tran2pg_parse(url = url , author = author , public_time= public_time, 
                            page_source= page_source, content= content, 
                            website_name= website_name, channel_name= channel_name, 
                            title= title, topic = topic, tag = tag,
                            meta_keywords = meta_keywords, flag = flag,
                            pic = pic)
        sess.add(_obj)
        sess.commit()
        print('完成入库')
        return uid
    except Exception as e:
        print(e)
        sess.rollback()
        return False
    finally:
        sess.close()    
    pass

        
def _result(result):
    return result is None 
@retry( wait_random_min=1000, wait_random_max=2000, retry_on_result=_result)
def crawl(url):
    '''抓取网页源码pageSource'''
    header = header={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
#                'Host': 'www.caam.org.cn',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
#                'Cookie': '__xwaf_id=e52bf9be294d90397354ab6d12a689eaa6fbbfae12e1338e0bac7e8b2b179e78; __xwaf_browser_auth=BkPxoimjrj8Xi8GRntg8Lw==; __xwaf_filter_key=57fdc8dd987c1627'
                }
    session = requests.session()
    try:
        ipusing = get_proxy_redis()
        ipusing=str(ipusing,encoding='utf-8')
#        _proxy = {'http':'http://%s'%ipusing,'https':'https://%s'%ipusing}
        log.info('now using %s'%ipusing)
        data = session.get(url, headers=header,  timeout=30)
        print("%s's status_code is %s" %(url, data.status_code))  # 打印相关url 的状态码
        if data.status_code == 200:
            data.encoding = data.apparent_encoding
            pageSource = data.text
            data.close()
            return pageSource
        elif data.status_code == 404:
            return '404'
    except Exception:
        pass
    finally:
        time.sleep(random.uniform(0, 2))        