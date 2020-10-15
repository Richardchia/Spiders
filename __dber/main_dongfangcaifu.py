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

from __dber.pg_client import session,session1
from __dber.pg_orm_dongfangcaifu import Jz_dongfangcaifu_PageSource, Jz_dongfangcaifu_content
from __utils.base import get_date_str,get_header,get_now_str,str_simhash,get_proxy_redis
sess=session()
sess1=session1()
class Start(Task):
    def run(self,params):
        #初始化的时候，选用页面长度为30
        #每日增量，只要一页就可以
        url_s1 = ('http://finance.eastmoney.com/news/cjjsp_%s.html','经济时评')
        url_s2 = ('http://finance.eastmoney.com/news/cgnjj_%s.html','国内经济')
        url_s3 = ('http://finance.eastmoney.com/news/cgjjj_%s.html','国际经济')
#        for i in range(1,4):
        for i in range(1,26):
            url1 = url_s1[0]%str(i)
            log.info('入队列 jz_cj_pagesource')
            queue_job('main_dongfangcaifu.Crawler1',
                      {'url':url1,'flag':url_s1[-1]},
                      queue = 'jz_cj_pagesource')
            url2 = url_s2[0]%str(i)
            log.info('入队列 jz_cj_pagesource')
            queue_job('main_dongfangcaifu.Crawler1',
                      {'url':url2,'flag':url_s2[-1]},
                      queue = 'jz_cj_pagesource')
            url3 = url_s3[0]%str(i)
            log.info('入队列 jz_cj_pagesource')
            queue_job('main_dongfangcaifu.Crawler1',
                      {'url':url3,'flag':url_s3[-1]},
                      queue = 'jz_cj_pagesource')
                
class Crawler1(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        print(url)
        try:
            ps = crawl(url)
            if len(str(ps))>500 :
                uid = store2pg(ps = ps,url = url,flag = flag)
            else:
                uid = None
            if uid:
                urls = list(re.compile('<p class="title">.*?<a href="(http:.*?)" target="_blank">',re.S).findall(ps))
                for u in urls:
                    url1 =u
                    log.info('入队列 jz_cj_pagesource')
                    queue_job('main_dongfangcaifu.Crawler2',
                              {'url':url1,'flag':flag},
                              queue = 'jz_cj_pagesource') 
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_cj_pagesource')
            queue_job('main_dongfangcaifu.Crawler1',
                      {'url':url,'flag':flag},
                      queue = 'jz_cj_pagesource')
            
class Crawler2(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess.query(Jz_dongfangcaifu_PageSource).filter_by(url = url).first()
            sess.rollback()
            if not info:
                ps = crawl(url)
                if len(str(ps))>500 and '返回' not in str(ps):
                    uid = store2pg(ps = ps,url = url,flag = flag)
                else:
                    uid = None
                if uid:
                    log.info('入队列 jz_cj_parse')
                    queue_job('main_dongfangcaifu.Parse',
                              {'url':url,'flag':flag},
                              queue = 'jz_cj_parse')
            else:
                print('新闻已存在')   
                log.info('入队列 jz_cj_parse')
                queue_job('main_dongfangcaifu.Parse',
                          {'url':url,'flag':flag},
                          queue = 'jz_cj_parse')
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_cj_pagesource')
            queue_job('main_dongfangcaifu.Crawler2',
                      {'url':url,'flag':flag},
                      queue = 'jz_cj_pagesource') 

class Parse(Task):
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess1.query(Jz_dongfangcaifu_content).filter_by(url = url, channel_name= flag).first()
            print(1)
            info_2 = sess.query(Jz_dongfangcaifu_PageSource).filter_by(url = url).first()
            print(2)
            if not info and info_2:
                ps = info_2.pagesource
                ps_uid = info_2.uid
                author = re.findall('data-source="(.*?)">',ps)
                author = author[0] if author else None                
                public_time = re.findall('<div class="time">(.*?)</div>',ps)
                public_time = public_time[0].replace('年','-').replace('月','-').replace('日','') if public_time else None
                content = re.compile('<!--文章主体-->(.*?)<!--原文标题-->',re.S).findall(ps)
                content2 = content[0] if content else None 
                if content2:
                    pic = re.findall('<img src="(https.*?)"',content2)
                    pic = ';'.join(pic)
                    content2 = content2.replace('<img src','[img src').replace('" />','" /]').replace('</p>','\n')
                else:
                    pic = ''
                content = re.sub('<.*?>','',content2.replace('&nbsp;','')).replace('\t','').replace(' ','').replace('\u3000','').replace('\n\n','\n').replace('本文版权为电动汽车网-电动邦所有，欢迎转载但请务必注明来源。','').strip()
                title = re.findall('<h1>(.*?)</h1>',ps)[0]
#                tag = re.findall('<a class="fn-left".*?target="_blank">(.*?)</a>',ps)
#                tag = ' '.join(tag)
                hid = store2pg_parse(url = url, author = author, public_time= public_time,
                                     page_source= ps_uid, content= content, 
                                     website_name= '东方财富网', channel_name= flag, title= title,
                                     topic = None,tag = None, meta_keywords = None,
                                     pic = pic,flag = None)
                if hid:
                    print('完成') 
            else:
                print('新闻解析已存在')  
        except Exception as e:
            print(e)
            if e!="'NoneType' object has no attribute 'replace'":
                print('重新入队')
                log.info('入队列 jz_cj_parse')
#                queue_job('main_dongfangcaifu.Parse',
#                      {'url':url,'flag':flag},
#                      queue = 'jz_cj_parse')
    
def tran2pg(ps=None, url=None, flag=None):
    '''
    转换为ORM obj
    '''
    crawlt = get_now_str()
    crawld = get_date_str()
    _hid=str_simhash(crawlt+url)
    _obj = Jz_dongfangcaifu_PageSource(uid=_hid,
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
    obj = Jz_dongfangcaifu_content( 
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
        sess1=session1()
        uid, _obj = tran2pg_parse(url = url , author = author , public_time= public_time, 
                            page_source= page_source, content= content, 
                            website_name= website_name, channel_name= channel_name, 
                            title= title, topic = topic, tag = tag,
                            meta_keywords = meta_keywords, flag = flag,
                            pic = pic)
        sess1.add(_obj)
        sess1.commit()
        print('完成入库')
        return uid
    except Exception as e:
        print(e)
        sess1.rollback()
        return False
    finally:
        sess1.close()
        
def _result(result):
    return result is None 
@retry( wait_random_min=1000, wait_random_max=2000, retry_on_result=_result)
def crawl(url):
    '''抓取网页源码pageSource'''
    header = get_header()
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
            return 404
    except Exception:
        pass
    finally:
        time.sleep(random.uniform(0, 2))        
