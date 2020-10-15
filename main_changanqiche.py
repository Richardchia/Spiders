# -*- coding: utf-8 -*-
"""
Created on Wed May 29 11:04:59 2019

@author: gz02399
"""

import __init__
import requests 
import re
import time
import datetime
import random
from mrq.task import Task
from mrq.job import queue_job
from mrq.context import log,traceback
#from retrying import retry

from __dber.pg_client import session,session1
from __dber.pg_orm_changanqiche import Jz_changanqiche_PageSource, Jz_changanqiche_content
from __utils.base import get_date_str,get_header,get_now_str,str_simhash,get_proxy_redis

sess=session()
sess1=session1()

class Start(Task):
    def run(self,params):
        params1 = ('https://www.changan.com.cn/news-changan?page=%s&year=%s&keyword=&type=0&ajax_req=1&t=1584689024944','长安动态')
        params2 = ('http://www.changan.com.cn/company.shtml','合资合作')
        
        #初始化的时候，选用页面长度为30
        for u in [params1]:
            if u[-1] == '长安动态':
                year = datetime.datetime.now().strftime('%Y')
                #每周一次，一次一页（8篇）
                for page in range(1,2):
                    url = u[0]%(str(page),str(year))
                    log.info('入队列 jz_qymh_pagesource')
                    queue_job('main_changanqiche.Crawler1',
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
            if len(ps)>10:
                uid = store2pg(ps = ps,url = url,flag = flag)
            else:
                uid = None
            if uid:
                urls =re.findall('"supdata_whereid":"(.*?)"',ps)
                for u in urls:
                    url = 'http://www.changan.com.cn/news-details.shtml?whereid=%s&column_id=98'%u
                    log.info('入队列 jz_qymh_pagesource')
                    queue_job('main_changanqiche.Crawler2',
                              {'url':url,'flag':flag},
                              queue = 'jz_qymh_pagesource') 
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qymh_pagesource')
            queue_job('main_changanqiche.Crawler1',
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
            info = sess.query(Jz_changanqiche_PageSource).filter_by(url = url).first()
            sess.rollback()
            if not info:
                ps = crawl(url)
                if len(ps)>100:
                    uid = store2pg(ps = ps,url = url,flag = flag)
                else:
                    uid = None
                if uid:
                    log.info('入队列 jz_qymh_parse')
                    queue_job('main_changanqiche.Parse',
                              {'url':url,'flag':flag},
                              queue = 'jz_qymh_parse')
            else:
                print('新闻已存在') 
                log.info('入队列 jz_qymh_parse')
                queue_job('main_changanqiche.Parse',
                          {'url':url,'flag':flag},
                          queue = 'jz_qymh_parse')
        except Exception as e:
            print(e)
            print('重新入队')
            log.info('入队列 jz_qymh_pagesource')
            queue_job('main_changanqiche.Crawler2',
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
            info = sess1.query(Jz_changanqiche_content).filter_by(url = url).first()
            sess.rollback()
            if not info:
                info_2 = sess.query(Jz_changanqiche_PageSource).filter_by(url = url).first()
                sess.rollback()
                ps = info_2.pagesource
                ps_uid = info_2.uid
#                author = re.findall('',ps)
#                author = author[0] if author else None
                public_time = re.findall('><span id="love_number">(.*?)</span',ps)
                public_time = public_time[0].strip() if public_time else None
                content1 = re.compile('<div class="news-details-main">(.*?)<div class="details-main-btn"',re.S).findall(ps)
                content2 = content1[0] if content1 else None 
                pic = re.findall('src="(.*?)" ',content2) if content2 else []
                for i in range(len(pic)):
                    pic[i] = 'https:'+pic[i]
                pic = ';'.join(set(pic))
                content2 = content2.replace('<br/>','\n').replace('<img src','[img src').replace('jpg"/>','jpg"/]')
                content = re.sub('<.*?>','',content2.replace('&nbsp;','')).replace('$(".content img").wrap("");','').strip()
                title = re.findall('<h2>(.*?)</h2>',ps)[0]
                
                meta_keywords = re.compile('<meta name="keywords" content="(.*?)">',re.S).findall(ps)[0].strip()
                hid = store2pg_parse(url = url, author = None, public_time= public_time,
                                     page_source= ps_uid, content= content, 
                                     website_name= '长安汽车', channel_name= flag, title= title,
                                     topic = None,tag = meta_keywords, meta_keywords = None,
                                     pic = pic,flag = None)
                if hid:
                    print('完成') 
            else:
                print('新闻解析已存在')  
        except Exception as e:
            print(e)
            if e!="'NoneType' object has no attribute 'replace'":
                print('重新入队')
                log.info('入队列 jz_qymh_parse')
                queue_job('main_changanqiche.Parse',
                          {'url':url,'flag':flag},
                          queue = 'jz_qymh_parse')
            
def tran2pg(ps=None, url=None, flag=None):
    '''
    转换为ORM obj
    '''
    crawlt = get_now_str()
    crawld = get_date_str()
    _hid=str_simhash(crawlt+url)
    _obj = Jz_changanqiche_PageSource(uid=_hid,
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
    sess = session
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
    obj = Jz_changanqiche_content( 
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
    sess1 = session1()
    try:
        uid, _obj = tran2pg_parse(url = url , author = author , public_time= public_time, 
                            page_source= 0, content= content, 
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
#@retry( wait_random_min=1000, wait_random_max=2000, retry_on_result=_result)
def crawl(url):
    '''抓取网页源码pageSource'''
    header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
#            'Cookie': 'sessionid=fa7146f72ec648cb82faf9e7e14a12fa; _ga=GA1.3.642028384.1560481142; _gid=GA1.3.977264262.1560481142; Hm_lvt_1011a3fe8c5e5a5c411464126962ccce=1560481142,1560482940; ci_session=d7o1puig7lct5e6k2j7ncnkdcg5qs571; Hm_lpvt_1011a3fe8c5e5a5c411464126962ccce=1560483416; pt_s_40c7e70d=vt=1560483415578&cad=; _gat=1; pt_40c7e70d=uid=HVgrEDeimT82eMv71tMmew&nid=0&vid=HQBRWERJtGa1RdMuBrMP-A&vn=3&pvn=3&sact=1560483426793&to_flag=0&pl=wo14skj6VTYF9oRehNWC7A*pt*1560483415578; SRV=11d3672a-d08e-4fcf-bba1-144054027590|XQMWa|XQMND',
            'Host': 'www.changan.com.cn',
            'Referer': 'http://www.changan.com.cn/news-changan.shtml',
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
        return '000'
    finally:
        time.sleep(random.uniform(0, 2))