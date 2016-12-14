# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 01:06:40 2016

@author: Toby
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Oct 23 15:42:00 2016

@author: Toby
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Oct 09 16:58:16 2016

@author: Toby
"""
import sys
sys.path.insert(0, r"E:")
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import pytz
#from zipline.finance.slippage import VolumeShareSlippage
from zipline.finance.slippage import FreeTradeSlippage
from zipline.dailydata import DBProxy
dbProxy = DBProxy.DBProxy()

#basic_eps = dbProxy._get_fundamentals({'BASICEPS':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
#revenue = dbProxy._get_fundamentals({'BIZINCO':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
#factor = dbProxy._get_fundamentals({'PARENETP':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
period_start = '20140101'
period_end = '20160330'  
#basic_eps = pd.read_pickle(r"D:\Data\basic_eps")
#revenue = pd.read_pickle(r"D:\Data\revenue")
#factor = pd.read_pickle(r"D:\Data\factor")
from zipline.api import(
get_fundamentals,
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
import zipline as zp
from zipline.analysis import resolve_orders
from zipline.analysis import plot

def initialize(context):
    set_slippage(FreeTradeSlippage())
    add_history(60, '1d','ret')
    context.timer=0
    add_history(60, '1d', 'volume')
    #set_slippage(FreeTradeSlippage())

def handle_data(context, data):
    today = get_datetime()
    if schedule(today):
        month = today.month
        year = today.year
        factor = calculate_factor(context, data)
        sid_to_drop = factor.isnull()
        sid_to_drop = list(sid_to_drop[sid_to_drop==True].index)
        factor = factor.drop(sid_to_drop)
        sids = factor.index

        factor_score = (factor - np.nanmean(factor))/np.nanstd(factor)
        total_score = pd.DataFrame(factor_score, columns = ['score'], index = sids)     

        vol = history(1, '1d', 'volume')
        vol = vol.ix[-1,:]
        active_sids = list(get_universe())
        total_score = total_score.ix[active_sids,:]
        total_score.sort(inplace = True, columns = 'score')
        total_score.dropna(axis = 0,subset = ['score'],inplace = True)
        low = total_score.ix[:len(total_score)/5,:]
        high = total_score.ix[-len(total_score)/5:,:]
        long_stocks = set(high.index)
        short_stocks = set(low.index)
    
        N1 = len(short_stocks)
        N2 = len(long_stocks)
        for sid in short_stocks:
            order_target_percent(sid, -1.0/N1) 
        for sid in long_stocks:
            order_target_percent(sid, 1.0/N2)
    long_value = 0
    short_value = 0 
    for key, pos in algo.portfolio.positions.iteritems():
        if pos.amount>0:
            long_value=long_value+pos['amount']*pos['last_sale_price']
        else:
            short_value=short_value+pos['amount']*pos['last_sale_price']
    record(long_value=long_value)
    record(short_value=short_value)
            
def schedule(date):
    if date.weekday() == 0 and (15<=date.day<=21):
        return True
    else:
        return False

def calculate_factor(context, data):
    ret_history = history(60, '1d','ret') 
    raw_momentum = ret_history.mean()  
    factor = (raw_momentum - raw_momentum.mean())/  raw_momentum.std() 
    return factor
period_start = '20150101'
period_end = '20160330'            
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '2070000060',
                     'warming_period':20,
                     'capital_base': 100000000.0,
                     'security_type': 'stock'
                     }           
algo = zp.TradingAlgorithm(**TradingDictionary)
results = algo.run(False) 