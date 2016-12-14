# -*- coding: utf-8 -*-
"""
Created on Mon May 09 23:45:20 2016

@author: Toby
"""
import pandas as pd
import pytz
import datetime
import pickle
import os


#input goes here
data_frequency = 'daily'
folder = r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\MarketConfig"
n = 2 #number of minutely benchmark files (not necessary if frequency is set to daily)

############################process#########################################
if data_frequency == 'daily':
    f = folder + r"\benchmark_daily.csv"
    benchmark = pd.read_csv(f,parse_dates=['Date'],date_parser = lambda x:pd.datetime.strptime(x,'%Y/%m/%d'),\
                index_col = 0)
    benchmark.index = map(lambda x:pytz.utc.localize(x), benchmark.index)
    ofile = os.path.join(folder, r'benchmark_daily.pkl')
else:
    prior_close = None
    series = pd.Series()
    for i in range(1, n + 1):
        f = folder + r"\benchmark_minute%s.csv"%i
        benchmark = pd.read_csv(f)
        rawdates = map(lambda x, y: "%s %s"%(x,y), benchmark['date'], benchmark['time'])
        dates = map(lambda x: datetime.datetime.strptime(x, "%Y/%m/%d %H:%M"), rawdates)
        benchmark.index = dates
        returns = benchmark['close'].pct_change()
        if i == 1:
            returns[0] = 0
        else:
            returns[0] = benchmark['close'].iloc[0]/prior_close - 1
        prior_close = benchmark['close'].iloc[-1]
        series= series.append(returns)
    benchmark = series
    benchmark.index = map(lambda x:pytz.utc.localize(x), benchmark.index)
    ofile = os.path.join(folder, r"benchmark_minute.pkl")
with open(ofile, 'w') as f:
    pickle.dump(benchmark, f)
        
f = os.path.join(folder, r"treasuries.csv")
ofile = os.path.join(folder, r"treasuries.pkl")
tr = pd.read_csv(f)  
tr.to_pickle(ofile)   
        
