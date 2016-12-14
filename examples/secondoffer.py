# -*- coding: utf-8 -*-
"""
Created on Sun Oct 09 16:58:16 2016

@author: Toby
"""
import sys
sys.path.insert(0, "E:\Anaconda\Lib\site-packages\zipline_china1.3")
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import pytz
from zipline.dailydata import DBProxy
dbProxy = DBProxy.DBProxy()
sql = "select B.DECLAREDATE, B.LISTDATE, SECODE, ISSPRICE from yunneng.PROADDISS as A \
inner join yunneng.LIMSKHOLDER as B where A.COMPCODE = B.COMPCODE and A.PUBLISHDATE = \
B.DECLAREDATE and B.DECLAREDATE > Date('20090101')"
res = dbProxy.doQuery(sql)
df = np.array(res)
df = pd.DataFrame(df, columns = ['dt','listdate','sid','issprice'])
#df = pd.read_csv(r"D:\Data\EventData\2ndoffer.csv")
df['dt'] = df['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(str(int(x)),r'%Y%m%d')))
df.ix[df['dt'].apply(lambda x:x.weekday()==5),'dt'] = df.ix[df['dt'].apply(lambda x:x.weekday()==5),'dt'] + timedelta(2)
df.ix[df['dt'].apply(lambda x:x.weekday()==5),'dt'] = df.ix[df['dt'].apply(lambda x:x.weekday()==5),'dt'] + timedelta(1)
df['listdate'] = df['listdate'].apply(lambda x: pytz.utc.localize(datetime.strptime(str(int(x)),r'%Y%m%d')))
df.index = df.dt
df = df.drop_duplicates(subset = ['sid','dt'])
df.sort(columns = ['dt'], inplace = True)


period_start = '20140101'
period_end = '20160331'




from zipline.api import(
history,
add_history,
set_long_only,
get_datetime,
get_universe,
cancel_order,
order_target_percent,
order_target_value,
record,
get_open_orders,
set_slippage,
)
from zipline.finance.slippage import FreeTradeSlippage
import zipline as zp
from zipline.analysis import resolve_orders
from zipline.analysis import plot

cap = dbProxy._get_sn_ts_optional({'TOTMKTCAP': float}, 'TQ_QT_SKDAILYPRICE', period_start, period_end)
def initialize(context):
    set_long_only()
    #set_slippage(FreeTradeSlippage())
    """
    闄愬畾涓嶈兘鍗栫┖
    """
    context.anchor_price = pd.Series()
    context.monitor = pd.Series()
    context.listdate = pd.Series()
    context.currentprice = pd.Series()
    context.upper = pd.Series()
    context.lower = pd.Series()
    add_history(1, '1d', 'TOTMKTCAP')
    add_history(1, '1d', 'price', ffill = True)

def handle_data(context, data):
    today = get_datetime()
    
    cap = history(1, '1d', 'TOTMKTCAP')
    cap = cap.iloc[-1,:]
    cap.sort(inplace = True)
    top500 = set(cap.index[-500:])
    lastday_price = history(1, '1d', 'price', ffill = True).iloc[-1,:]
  
    
    context.anchor_price = context.anchor_price.reindex(index = list(get_universe()))
    context.monitor = context.monitor.reindex(index = list(get_universe()), fill_value = 0)
    context.listdate = context.listdate.reindex(index = list(get_universe()), fill_value = pytz.utc.localize(datetime(2000,1,1)))
    context.currentprice = context.currentprice.reindex(index = list(get_universe()), fill_value = 1)
    context.upper = context.upper.reindex(index = list(get_universe()), fill_value = 0.95)
    context.lower = context.lower.reindex(index = list(get_universe()), fill_value = 0.0)
    for sid in get_universe():
        try:
            event = data[sid]
        except:
            continue
        if 'issprice' in event:
            if context.monitor[sid] == 0:
                context.monitor[sid] = 1
                context.anchor_price[sid] = float(event['issprice'])/lastday_price[sid]
                context.listdate[sid] = event['listdate']

    filter1 = pd.Series()
    filter1 = filter1.reindex(index = list(get_universe()), fill_value = False)
    
    for sid in get_universe():       
        try:
            event = data[sid]
        except:
            continue
        if 'ret' in event and context.monitor[sid] == 1:
            context.currentprice[sid] = context.currentprice[sid] * (event['ret']/100.0+1)
            if context.lower[sid] < context.currentprice[sid] <= context.upper[sid]*context.anchor_price[sid] and today < context.listdate[sid]:
                filter1[sid] = True
    filtered = filter1
    selected = set(filtered[filtered == True].index)
    selected = selected.intersection(top500)
    
    context.currentprice[context.listdate <= today] = 1
    context.monitor[context.listdate <= today] = 0
    
    
    sell =  list()
    portfolio = context.portfolio
    for pos in [pos for pos in portfolio.positions.itervalues() if pos.amount >= 100]:
        if pos.last_sale_price <= 0.9*pos.cost_basis:
            sell.append(pos.sid) 
            context.lower[pos.sid] = 0.93
        if context.listdate[pos.sid] + timedelta(days=60) <= today:
            sell.append(pos.sid)
        if context.currentprice[pos.sid] >= 1.0:
            sell.append(pos.sid)
        
    sell = set(sell)
    existing_sids = set([sid for sid, pos in portfolio.positions.iteritems() if pos.amount >= 100])
    remaining = existing_sids.difference(sell)
    new = selected.difference(remaining)
    N = len(new) + len(remaining)
    for sid in sell:
        order_target_value(sid, 0.0) 
    for sid in remaining.union(new):
        order_target_percent(sid, 1.0/N)


TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '000300.SH',
                     'warming_period':2,
                     'capital_base': 10000000
                     }           
algo = zp.TradingAlgorithm(**TradingDictionary)
results = algo.run(False, cap, df) 
