# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 17:42:46 2018

@author: keishun
"""

import __init__
import psycopg2
import traceback


# from bbsKb import Base, BBSKb_url
from pg_orm_qichegongyexiehui import Base  #从cwzPost下面导入Base，在下面的执行中只会创建在此脚本中的table表
from pg_client import engine
# from frame.orm.config import session
# from frame.orm.kbPost import BbsCarList
Base.metadata.create_all(engine)
print('完成数据表的创建')
