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
from zipline.finance.slippage import FreeTradeSlippage
from zipline.dailydata import DBProxy
dbProxy = DBProxy.DBProxy()

#basic_eps = dbProxy._get_fundamentals({'BASICEPS':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
revenue = dbProxy._get_fundamentals2({'TAGRT':float}, 'TQ_FIN_PROINDICDATA')
netincome = dbProxy._get_fundamentals2({'NPGRT':float}, 'TQ_FIN_PROINDICDATA')
period_start = '20140101'
period_end = '20160630'  
sector = dbProxy.doQuery("select SECODE, SWLEVEL2CODE from finchina.TQ_SK_BASICINFO where EXCHANGE in ('001002', '001003')")
sector = pd.DataFrame(np.array(sector), columns = ['sid','sector'])
sector.index = sector.sid.copy()
#basic_eps = pd.read_pickle(r"D:\Data\basic_eps")
#revenue = pd.read_pickle(r"D:\Data\revenue")
#netincome = pd.read_pickle(r"D:\Data\netincome")
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
from zipline.finance.slippage import FreeTradeSlippage
import zipline as zp
from zipline.analysis import resolve_orders
from zipline.analysis import plot

def initialize(context):
    set_long_only()
    set_slippage(FreeTradeSlippage())
    context.sector = sector
    context.timer=0
    add_history(40, '1d', 'volume')
    #set_slippage(FreeTradeSlippage())

def handle_data(context, data):
    today = get_datetime()
    if schedule(today):
        month = today.month
        year = today.year
        basic_eps = get_fundamentals('BASICEPS')
        revenue = get_fundamentals('BIZINCO')
        netincome = get_fundamentals('PARENETP')
        #sid_to_drop1 = basic_eps.isnull().all(axis=1)
        sid_to_drop2 = revenue.isnull().all(axis=1)
        sid_to_drop3 = netincome.isnull().all(axis=1)
        sid_to_drop = (sid_to_drop2|sid_to_drop3)
        sid_to_drop = list(sid_to_drop[sid_to_drop==True].index)
        basic_eps = basic_eps.drop(sid_to_drop)
        revenue = revenue.drop(sid_to_drop)
        netincome = netincome.drop(sid_to_drop)
        sids = basic_eps.index
        if month in (1,2,3,4):
            rprtdate11,rprtdate12 = str(year-1)+'0930', str(year-2)+'0930'
            rprtdate21,rprtdate22 = str(year-2)+'1231', str(year-3)+'1231'
            rprtdate31,rprtdate32 = str(year-3)+'1231', str(year-4)+'1231'
            rprtdate41,rprtdate42 = str(year-4)+'1231', str(year-5)+'1231'
            rprtdate51,rprtdate52 = str(year-5)+'1231', str(year-6)+'1231'

        elif month in (5,6,7,8):
            rprtdate11,rprtdate12 = str(year)+'0331', str(year-1)+'0331'
            rprtdate21,rprtdate22 = str(year-1)+'1231', str(year-2)+'1231'
            rprtdate31,rprtdate32 = str(year-2)+'1231', str(year-3)+'1231'
            rprtdate41,rprtdate42 = str(year-3)+'1231', str(year-4)+'1231'
            rprtdate51,rprtdate52 = str(year-4)+'1231', str(year-5)+'1231'

        elif month in (9,10):
            rprtdate11,rprtdate12 = str(year)+'0630', str(year-1)+'0630'
            rprtdate21,rprtdate22 = str(year-1)+'1231', str(year-2)+'1231'
            rprtdate31,rprtdate32 = str(year-2)+'1231', str(year-3)+'1231'
            rprtdate41,rprtdate42 = str(year-3)+'1231', str(year-4)+'1231'
            rprtdate51,rprtdate52 = str(year-4)+'1231', str(year-5)+'1231'
 
        elif month in (11,12):
            rprtdate11,rprtdate12 = str(year)+'0930', str(year-1)+'0930'
            rprtdate21,rprtdate22 = str(year-1)+'1231', str(year-2)+'1231'
            rprtdate31,rprtdate32 = str(year-2)+'1231', str(year-3)+'1231'
            rprtdate41,rprtdate42 = str(year-3)+'1231', str(year-4)+'1231'
            rprtdate51,rprtdate52 = str(year-4)+'1231', str(year-5)+'1231'
        
        rprtdates = [rprtdate11, rprtdate21, rprtdate31, rprtdate41, rprtdate51]
        #rprtdates = map(lambda x:datetime.strptime(x,r'%Y%m%d'), rprtdates)
        #rprtdates2 = [rprtdate12, rprtdate22, rprtdate32, rprtdate42, rprtdate52]
        #rprtdates2 = map(lambda x:datetime.strptime(x,r'%Y%m%d'), rprtdates2)
        #eps = (basic_eps[rprtdates].astype(float).values - basic_eps[rprtdates2].astype(float).values)/basic_eps[rprtdates2].astype(float).values
        revenue = revenue[rprtdates].astype(float).values
        netincome = netincome[rprtdates].astype(float).values

        #eps_score = np.nanmean(eps,axis=1)*0.7 - np.nanstd(eps,axis=1)*0.3
        revenue_score = np.nanmean(revenue,axis=1)*0.7 - np.nanstd(revenue,axis=1)*0.3
        netincome_score = np.nanmean(netincome,axis=1)*0.7 - np.nanstd(netincome,axis=1)*0.3
        #eps_score = (eps_score - eps_score.mean())/eps_score.std()
        #revenue_score = (revenue_score - revenue_score.mean())/revenue_score.std()
        #netincome_score = (netincome_score - netincome_score.mean())/netincome_score.std()
        total_score = revenue_score + netincome_score
        total_score = pd.DataFrame(total_score, columns = ['score'], index = sids)
        #total_score['eps_mean'] = np.nanmean(eps,axis=1)
        total_score['revenue_mean'] = np.nanmean(revenue,axis=1)
        total_score['netincome_mean'] = np.nanmean(netincome,axis=1)
        total_score['revenue_grt'] = revenue[:,0]
        total_score['netincome_grt'] = netincome[:,0]
        total_score['sector'] = context.sector['sector']
        all_sectors = set(context.sector['sector'].values)
        selected = set()
        for sec in all_sectors:
            score_sec = total_score[total_score['sector'] == sec].copy()
            mid = score_sec['score'].median()
            sec_filter = (score_sec['score']>mid)&(score_sec['revenue_mean']>0)&(score_sec['netincome_mean']>0)\
            &(score_sec['revenue_grt']>=0.15)&(score_sec['netincome_grt']>=0.15)
            selected = selected.union(set(sec_filter[sec_filter==True].index))
        active_sids = get_universe()
        selected = selected.intersection(active_sids)
        vol = history(20, '1d', 'volume')
        meanvol = vol.mean(axis = 0) 
        meanvol = meanvol[list(selected)]
        meanvol = meanvol[meanvol>0]
        meanvol.sort(inplace = True)
        selected = set(meanvol.index[0:20])

        portfolio = context.portfolio
        existing_sids = set([sid for sid, pos in portfolio.positions.iteritems() if pos.amount >= 100])
        sell = existing_sids.difference(selected)
        remaining = existing_sids.difference(sell)
        new = selected.difference(remaining)
        N = len(new) + len(remaining)
        if N is not 20:
            print "warning: the number of selected stocks is not 20!\n date:%s"%datetime.strftime(today,r'%Y/%m/%d')
        for sid in sell:
            order_target_value(sid, 0.0) 
        for sid in remaining.union(new):
            order_target_percent(sid, 1.0/N)
def schedule(date):
    if date.weekday() == 0 and (8<=date.day<=14):
        return True
    else:
        return False
         
period_start = '20150901'
period_end = '20161130'            
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '2070000060',
                     'warming_period':20,
                     'capital_base': 10000000,
                     'security_type': 'stock'
                     }           
algo = zp.TradingAlgorithm(**TradingDictionary)
results = algo.run(False, fundamental_data = [basic_eps, revenue, netincome]) 