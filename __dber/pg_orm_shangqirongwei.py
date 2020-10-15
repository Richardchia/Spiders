# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 17:38:28 2018

@author: keishun
"""

from sqlalchemy import Column,String,Boolean,TIMESTAMP,VARCHAR,Integer,Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pg_client import engine

Base = declarative_base()
metadata = Base.metadata

class Jz_shangqirongwei_PageSource(Base):
    __tablename__ = 'jz_shangqirongwei_pagesource'
#    __table_args__ = {"useexisting": False}
    uid = Column(String(),primary_key = True)
    url = Column(String())  
    collection_time = Column(TIMESTAMP(),nullable = True,server_default = func.now())  #爬取时间
    pagesource = Column(VARCHAR())
    flag= Column(String()) #标志位，纪录网站下的各站点
    write_date=Column(Date())

class Jz_shangqirongwei_content(Base):
    __tablename__ = 'jz_shangqirongwei_content'
    __table_args__ = {"useexisting": False}
    uid = Column(Integer(), primary_key=True)    # 自增序号 not null
    url = Column(String(), nullable=True)   # 网页的url not null
    author = Column(VARCHAR(200))   # 作者 null
    public_time = Column(TIMESTAMP())   # 内容发布时间 null
    collection_time = Column(TIMESTAMP(),nullable = True,server_default = func.now())  # 首次采集时间 not null
    update_time = Column(TIMESTAMP())   # 最近更新时间
    page_source = Column(String(), nullable=True)  # 为t_pagesource_list中的hid
    content = Column(String())  # 解析后的内容 null
    website_name = Column(VARCHAR(200))  # 站点名（如：微信公众号）
    channel_name = Column(VARCHAR(200))  # 频道名（如：微信公众号下的汽车之家）
    title = Column(VARCHAR(300))    # 标题
    topic = Column(VARCHAR(300))    # 话题
    tag = Column(String())  # 显示标签
    meta_keywords = Column(String())    # 网页<head>头部下的<meta Keywords>
    write_date=Column(Date())
    flag = Column(String()) 
    pic = Column(String()) 
    
if __name__=='__main__':
    Base.metadata.create_all(engine)
    print('完成数据表的创建')