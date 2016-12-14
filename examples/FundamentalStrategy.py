# -*- coding: utf-8 -*-
"""
Created on Mon Apr 04 00:55:35 2016

@author: Toby
"""

import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china")
import pandas as pd
import datetime
import pytz

data = pd.read_csv("D:\Data\cleandata3.csv")
data.index = data.Date
data = data.drop('Date',axis=1)
data.index = map(lambda x:datetime.datetime.strptime(x,"%Y/%m/%d"),data.index)
data.index = map(lambda x:pytz.utc.localize(x),data.index)

Ind = pd.read_csv("D:\Data\\benchmark.csv")
Ind.index = Ind.Date
Ind = Ind.drop('Date',axis=1)
Ind.index = map(lambda x:datetime.datetime.strptime(x,"%Y/%m/%d"),Ind.index)
Ind.index = map(lambda x:pytz.utc.localize(x),Ind.index)

from zipline.api import (
    add_fundamental,
    get_fundamentals,
    order_target,
    record,
    symbol,
)
import zipline as zp

def initialize(context):
    # Register 2 histories that track daily prices,
    # one with a 100 window and one with a 300 day window
    add_fundamental(1, 'nagy')
    context.i = 0
    
def handle_data(context, data):
    # Skip first 300 days to get full windows

    # Compute averages
    # history() has to be called with the same params
    # from above and returns a pandas dataframe.
    for sym in context.my_universe:
        order_target(sym, 100)
    record(context.nagy)
    # Save values for later inspection

           
def before_trading_start(context, data):
    fundamental_df = get_fundamentals('nagy', 2)
    context.my_universe = []
    context.nagy = fundamental_df
    for stock in fundamental_df:
        nagy_latest = fundamental_df[stock][fundamental_df.index[-1]]
        nagy_previous = fundamental_df[stock][fundamental_df.index[-2]]
        if nagy_latest > nagy_previous:
            context.my_universe.append(stock)
            
algo = zp.TradingAlgorithm(initialize=initialize, handle_data=handle_data, before_trading_start = before_trading_start)
results = algo.run(data, benchmark_return_source = Ind)