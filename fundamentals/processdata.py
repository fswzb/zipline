# -*- coding: utf-8 -*-
"""
Created on Fri Apr 01 00:03:55 2016

@author: Toby
"""

import pandas as pd
net_asset_growth = pd.read_csv("D:\Data\Fundamentals\\net_asset_growth_year.csv")
net_asset_growth.columns = ['sym','name', 200401,200402,200403,200404,200501,200502,200503,200504\
,200601,200602,200603,200604,200701,200702,200703,200704,200801,200802,200803,200804\
,200901,200902,200903,200904,201001,201002,201003,201004,201101,201102,201103,201104\
,201201,201202,201203,201204,201301,201302,201303,201304,201401,201402,201403,201404\
,201501,201502,201503,201504]
net_asset_growth = net_asset_growth.drop('name', axis = 1)
net_asset_growth.index = net_asset_growth['sym']
net_asset_growth = net_asset_growth.drop('sym', axis = 1)
net_asset_growth = net_asset_growth.T

fundamental_data = net_asset_growth