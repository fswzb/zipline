# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 00:59:08 2016

@author: Toby
"""
import pandas as pd
import numpy as np
from zipline.dailydata import DBProxy
dbProxy = DBProxy.DBProxy()
def resolve_orders(path, results):
    mapping = dbProxy._get_secode_mapping()
    time = list()
    sids = list()
    amount = list()
    commission = list()
    status = list()
    for orders in results.orders:
        if orders:
            for o in orders:
                if o['status'] is 1:
                    time.append(o['created'])
                    sids.append(o['sid'])
                    amount.append(o['amount'])
                    commission.append(o['commission'])
                    status.append(o['status'])
                
    order_df = pd.DataFrame({'time':time,'sid':sids,'amount':amount,'commission':commission, 'status':status})
    order_df['symbol'] = np.nan
    for idx, row in order_df.iterrows():
        sid = row['sid']
        symbol = mapping.ix[str(sid)].symbol
        l = len(str(symbol))
        if l<6:
            symbol = (6-l)*'0'+str(symbol)
        order_df.ix[idx, 'symbol'] = symbol
    order_df['time'] = order_df['time'].apply(lambda x:x.strftime(r"%Y/%m/%d"))
    order_df.to_excel(path, sheet_name='sheet1')
    return order_df