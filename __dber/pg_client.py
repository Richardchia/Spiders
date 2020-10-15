# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 17:41:35 2018

@author: keishun
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import psycopg2

#from __config.base_conf import pg_conf_1

#engine = create_engine('postgresql+psycopg2://postgres:pwdpostgres@192.168.253.64:10000/tpy')#备用，在本地调试的时候用
engine = create_engine('postgresql+psycopg2://postgres:pwdpostgres@139.159.236.51:10000/tpy')
#engine = create_engine('postgresql+psycopg2://postgres:oio@139.159.160.205:5432/tpy')
#engine1 = create_engine('postgresql+psycopg2://postgres:pwdpostgres@172.31.254.142:5435/tpy')
#engine1 = create_engine('postgresql+psycopg2://postgres:pwdpostgres@172.31.254.142:5435/jzqb')
engine1 = create_engine('postgresql+psycopg2://postgres:pwdpostgres@192.168.253.64:10000/jzqb')
session = sessionmaker(bind = engine)
session1 = sessionmaker(bind = engine1)