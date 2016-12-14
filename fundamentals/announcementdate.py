# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 22:03:47 2016

@author: Toby
"""

import pandas as pd
from datetime import datetime

announcement_date = pd.read_csv("D:\Data\Fundamentals\\announcement_date.csv")

announcement_date.columns=['sym','name',200401,200402,200403,200404,200501,200502,200503,200504\
,200601,200602,200603,200604,200701,200702,200703,200704,200801,200802,200803,200804\
,200901,200902,200903,200904,201001,201002,201003,201004,201101,201102,201103,201104\
,201201,201202,201203,201204,201301,201302,201303,201304,201401,201402,201403,201404\
,201501,201502,201503,201504]
announcement_date = announcement_date.drop('name', axis = 1)
announcement_date.index = announcement_date['sym']
announcement_date = announcement_date.drop('sym', axis = 1)
announcement_date = announcement_date.fillna('2020-01-01')

values = [map(lambda x:datetime.strptime(x, "%Y-%m-%d"), s) for s in announcement_date.values]
announcement_date = pd.DataFrame(values, index = announcement_date.index, columns = announcement_date.columns)
announcement_date = announcement_date.T
