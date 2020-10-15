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
from __dber.pg_orm_lingkeqiche import Jz_lingkeqiche_PageSource, Jz_lingkeqiche_content
from __utils.base import get_date_str,get_header,get_now_str,str_simhash,get_proxy_redis

sess=session()

class Start(Task):
    def run(self,params):
        params1 = ('https://www.lynkco.com.cn/Brand/News/NewsMore?pageIndex=%s','新闻潮讯')
        #初始化的时候，选用页面长度为30
        for u in [params1]:
            if u[-1] == '新闻潮讯':
                end = 8
            for i in range(1,end):
                url = u[0]%str(i)
                log.info('入队列 jz_qymh_pagesource')
                queue_job('main_lingkeqiche.Crawler1',
                          {'url':url,'flag':u[-1]},
                          queue = 'jz_qymh_pagesource')
                
class Crawler1(Task):
    '''
    新闻列表页
    '''
    def run(self,params):
        url = params['url']
        flag = params['flag']
        print(url)
        try:
            ps = crawl(url)
            print(len(ps))
            if len(ps)>100:
                uid = store2pg(ps = ps,url = url,flag = flag)
            else:
                uid = None
            if uid:
                urls =re.findall('<a class="newsLink" href="(.*?)">',ps)
                for u in urls:
                    url = 'https://www.lynkco.com.cn'+u
                    log.info('入队列 jz_qymh_pagesource')
                    queue_job('main_lingkeqiche.Crawler2',
                              {'url':url,'flag':flag},
                              queue = 'jz_qymh_pagesource') 
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qymh_pagesource')
            queue_job('main_lingkeqiche.Crawler1',
                      {'url':url,'flag':flag},
                      queue = 'jz_qymh_pagesource')
            
class Crawler2(Task):
    '''
    新闻详情页
    '''
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess.query(Jz_lingkeqiche_PageSource).filter_by(url = url).first()
            if not info:
                ps = crawl(url)
                if len(ps)>100:
                    uid = store2pg(ps = ps,url = url,flag = flag)
                else:
                    uid = None
                if uid:
                    log.info('入队列 jz_qymh_parse')
                    queue_job('main_lingkeqiche.Parse',
                              {'url':url,'flag':flag},
                              queue = 'jz_qymh_parse')
            else:
                print('新闻已存在') 
                log.info('入队列 jz_qymh_parse')
                queue_job('main_lingkeqiche.Parse',
                          {'url':url,'flag':flag},
                          queue = 'jz_qymh_parse')
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qymh_pagesource')
            queue_job('main_lingkeqiche.Crawler2',
                      {'url':url,'flag':flag},
                      queue = 'jz_qymh_pagesource') 
        
class Parse(Task):
    '''
    新闻解析
    '''
    def run(self,params):
        url = params['url']
        flag = params['flag']
        try:
            info = sess.query(Jz_lingkeqiche_content).filter_by(url = url).first()
            if not info:
                info_2 = sess.query(Jz_lingkeqiche_PageSource).filter_by(url = url).first()
                ps = info_2.pagesource
                ps_uid = info_2.uid
#                author = re.findall('',ps)
#                author = author[0] if author else None
                public_time = re.findall('<span>&nbsp;(.*?)</span></p>',ps)
                public_time = public_time[0].replace('.','-') if public_time else None
                content1 = re.compile('<div class="svg-btn">.*?<div class="share-content">',re.S).findall(ps)
                content2 = content1[0] if content1 else None 
                content = re.sub('<.*?>','',content2.replace('&nbsp;','')).replace('$(".content img").wrap("");','').strip()
                string = re.compile('●.*?\]',re.S).findall(content)[0] if re.compile('●.*?\]',re.S).findall(content) else ''
                content = content.replace(string,'').replace('\r\n','').replace(' ','').replace('返回','').strip()
                title = re.findall('<title>(.*?)</title>',ps)[0]
                pic = re.findall('src="(.*?)" ',content2) if content2 else []
                pic = ';'.join(pic)
                meta_keywords = re.compile('<meta name="keywords" content="(.*?)">',re.S).findall(ps)[0].strip()
                hid = store2pg_parse(url = url, author = None, public_time= public_time,
                                     page_source= ps_uid, content= content, 
                                     website_name= '领克汽车', channel_name= flag, title= title,
                                     topic = None,tag = None, meta_keywords = meta_keywords,
                                     pic = pic,flag = None)
                if hid:
                    print('完成') 
            else:
                print('新闻解析已存在')  
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qymh_parse')
            queue_job('main_lingkeqiche.Parse',
                      {'url':url,'flag':flag},
                      queue = 'jz_qymh_parse')
            
def tran2pg(ps=None, url=None, flag=None):
    '''
    转换为ORM obj
    '''
    crawlt = get_now_str()
    crawld = get_date_str()
    _hid=str_simhash(crawlt+url)
    _obj = Jz_lingkeqiche_PageSource(uid=_hid,
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
        print('入库')
        return uid
    except Exception as e:
        print(e)
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
    obj = Jz_lingkeqiche_content( 
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
        
def _result(result):
    return result is None 
@retry( wait_random_min=1000, wait_random_max=2000, retry_on_result=_result)
def crawl(url):
    '''抓取网页源码pageSource'''
    header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'HWWAFSESID=904a479c68ee5c31dc; HWWAFSESTIME=1560409827207; ASP.NET_SessionId=nkngwbs00eyd2yckuvpwjn4x; _gscu_1682711596=60409830clka1a38; _gscbrs_1682711596=1; SC_ANALYTICS_GLOBAL_COOKIE=e7493b9e002e43afacbcd31f626e208e|True; _gscs_1682711596=60409830b97u0d38|pv:6',
            'Host': 'www.lynkco.com.cn',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
            }
    session = requests.session()
    try:
#        ipusing = get_proxy_redis()
#        ipusing=str(ipusing,encoding='utf-8')
#        _proxy = {'http':'http://%s'%ipusing,'https':'https://%s'%ipusing}
#        log.info('now using %s'%ipusing)
        data = session.get(url, headers=header, timeout=30)
        print("%s's status_code is %s" %(url, data.status_code))  # 打印相关url 的状态码
        if data.status_code == 200:
            data.encoding = data.apparent_encoding
            pageSource = data.text
            data.close()
            print('here')
            return pageSource
        elif data.status_code != 200:
            return '404'
    except Exception as e:
        print(e)
    finally:
        time.sleep(random.uniform(0, 2))        