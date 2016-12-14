# -*- coding: utf-8 -*-
"""
Created on Tue Apr 05 20:43:27 2016

@author: Toby
"""

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
import pickle
with open('D:\Data\hfData') as f:
    data = pickle.load(f)


from zipline.api import (
    add_history,
    add_fundamental,
    history,
    get_fundamentals,
    order_target,
    record,
    symbol,
)
import zipline as zp
from zipline.hfdata.dataGen import generate_source

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
    print "into before_trading_start"
    fundamental_df = get_fundamentals('nagy', 2)
    print "got fundamental data"
    context.my_universe = []
    context.nagy = fundamental_df
    for stock in fundamental_df:
        nagy_latest = fundamental_df[stock][fundamental_df.index[-1]]
        nagy_previous = fundamental_df[stock][fundamental_df.index[-2]]
        if nagy_latest > nagy_previous:
            context.my_universe.append(stock)
    print "done with before_trading_start"

algo = zp.TradingAlgorithm(initialize=initialize, handle_data=handle_data, before_trading_start = before_trading_start,\
 data_frequency = "minute", emission_rate = "minute")
#directory = "D:\\Data\\MinuteData\\2015\\2015.7-2015.12\\2015.7-2015.12_new"
#data = generate_source(directory) 
results = algo.run(data)