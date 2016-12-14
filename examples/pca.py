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
from sklearn.decomposition import PCA
#from zipline.finance.slippage import VolumeShareSlippage
from zipline.finance.slippage import FreeTradeSlippage
from zipline.dailydata import DBProxy
dbProxy = DBProxy.DBProxy()

#basic_eps = dbProxy._get_fundamentals({'BASICEPS':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
#revenue = dbProxy._get_fundamentals({'BIZINCO':float}, 'TQ_FIN_PROINCSTATEMENTNEW')
factor1 = dbProxy._get_fundamentals({'BIZTOTINCO':float}, 'TQ_FIN_PROINCSTATEMENTNEW') #营业总收入
factor2 = dbProxy._get_fundamentals({'BIZINCO':float}, 'TQ_FIN_PROINCSTATEMENTNEW') #营业收入
factor3 = dbProxy._get_fundamentals({'PERPROFIT':float}, 'TQ_FIN_PROINCSTATEMENTNEW') #营业利润
factor4 = dbProxy._get_fundamentals({'TOTPROFIT':float}, 'TQ_FIN_PROINCSTATEMENTNEW') #利润总额
factor5 = dbProxy._get_fundamentals({'PARENETP':float}, 'TQ_FIN_PROINCSTATEMENTNEW') #归属母公司所有者净利润

period_start = '20140101'
period_end = '20160630'  
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
    context.timer=0
    add_history(1, '1d', 'volume')
    #set_slippage(FreeTradeSlippage())

def handle_data(context, data):
    today = get_datetime()
    if schedule(today):
        month = today.month
        year = today.year

        factor1 = get_fundamentals('BIZTOTINCO')
        factor1 = process_factor(factor1)
        factor2 = get_fundamentals('BIZINCO')
        factor2 = process_factor(factor2)
        factor3 = get_fundamentals('PERPROFIT')
        factor3 = process_factor(factor3)
        factor4 = get_fundamentals('TOTPROFIT')
        factor4 = process_factor(factor4)
        factor5 = get_fundamentals('PARENETP')
        factor5 = process_factor(factor5)   
        factor_index = factor1.index
        active_sids = list(get_universe())
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
        rprtdates2 = [rprtdate12, rprtdate22, rprtdate32, rprtdate42, rprtdate52]
        
        factor1 = (factor1[rprtdates].astype(float).values - factor1[rprtdates2].astype(float).values)/factor1[rprtdates2].astype(float).values
        factor1[factor1==np.inf]=1
        factor1[np.isnan(factor1).sum(axis=1)>=(factor1.shape[1]-1),:]=np.nan
        factor2 = (factor2[rprtdates].astype(float).values - factor2[rprtdates2].astype(float).values)/factor2[rprtdates2].astype(float).values
        factor2[factor2==np.inf]=1
        factor2[np.isnan(factor2).sum(axis=1)>=(factor2.shape[1]-1),:]=np.nan
        factor3 = (factor3[rprtdates].astype(float).values - factor3[rprtdates2].astype(float).values)/factor3[rprtdates2].astype(float).values
        factor3[factor3==np.inf]=1
        factor3[np.isnan(factor3).sum(axis=1)>=(factor3.shape[1]-1),:]=np.nan
        factor4 = (factor4[rprtdates].astype(float).values - factor4[rprtdates2].astype(float).values)/factor4[rprtdates2].astype(float).values
        factor4[factor4==np.inf]=1
        factor4[np.isnan(factor4).sum(axis=1)>=(factor4.shape[1]-1),:]=np.nan
        factor5 = (factor5[rprtdates].astype(float).values - factor5[rprtdates2].astype(float).values)/factor5[rprtdates2].astype(float).values
        factor5[factor5==np.inf]=1
        factor5[np.isnan(factor5).sum(axis=1)>=(factor5.shape[1]-1),:]=np.nan
        factor1 = np.nanmean(factor1,axis=1)*0.7 - np.nanstd(factor1,axis=1)*0.3
        factor2 = np.nanmean(factor2,axis=1)*0.7 - np.nanstd(factor2,axis=1)*0.3
        factor3 = np.nanmean(factor3,axis=1)*0.7 - np.nanstd(factor3,axis=1)*0.3
        factor4 = np.nanmean(factor4,axis=1)*0.7 - np.nanstd(factor4,axis=1)*0.3
        factor5 = np.nanmean(factor5,axis=1)*0.7 - np.nanstd(factor5,axis=1)*0.3
        factor = np.array([factor1,factor2,factor3,factor4,factor5]).T
        factor = factor[~np.isnan(factor).any(axis=1)]
        factor_index = factor_index[~np.isnan(factor).any(axis=1)]
        [u,s,v] = np.linalg.svd(factor)
        #pca = PCA(n_components=1)
        #pca.fit(factor)
        #pc = factor.dot(pca.components_.T)
        pc = np.dot(u[:,:len(s)],np.diag(s))
        factor_score = pc[:,1]
        total_score = pd.DataFrame(factor_score, columns = ['score'], index = factor_index)     
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
def schedule(date):
    if date.weekday() == 0 and (15<=date.day<=21):
        return True
    else:
        return False
def process_factor(factor):
    sid_to_drop = factor.isnull().all(axis=1)
    sid_to_drop = list(sid_to_drop[sid_to_drop==True].index)
    factor = factor.drop(sid_to_drop) 
    return factor
         
period_start = '20150901'
period_end = '20161130'           
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
results = algo.run(False, fundamental_data = [factor1, factor2, factor3, factor4, factor5]) 
import zipline.analysis.show_results as show
results_count = show.count(results,list(algo.perf_tracker.cumulative_risk_metrics.benchmark_returns))
show.plot_results(results_count)