# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 22:52:29 2016

@author: Toby
"""

import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china")
import datetime
import pytz
import pickle
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

#from zipline.hfdata.dataGen import processfiles
#from zipline.hfdata.dataGen import generate_source
#outdirectory = r'D:\Data\MinuteData\2015\2015.7-2015.12\2015.7-2015.12_new'
#data = generate_source(outdirectory)

#market data and config
market_config_location = r'D:\Data\MarketConfig'
with open('D:\Data\hfData') as f:
    data = pickle.load(f)
#fundamental data
fundamental_folder = r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\Fundamentals"
announcement_date_file = r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\Fundamentals\announcement_date.csv"
#backtest setting"
data_frequency = 'minute'
emission_rate = 'minute'
period_start = pytz.utc.localize(datetime.datetime.strptime("2015/09/01",'%Y/%m/%d'))
period_end = pytz.utc.localize(datetime.datetime.strptime("2015/11/01",'%Y/%m/%d'))

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
    sym = '002624.SZ'
    if context.buysignal == 1:
        order_target(sym, 100)
    record(context.nagy)
    # Save values for later inspection

           
def before_trading_start(context, data):
    print "into before_trading_start"
    fundamental_df = get_fundamentals('nagy', 1)
    print "got fundamental data"
    context.my_universe = ['002624.SZ']
    context.nagy = fundamental_df
    stock = '002624.SZ'
    nagy_latest = fundamental_df[stock][fundamental_df.index[-1]]
    nagy_previous = fundamental_df[stock][fundamental_df.index[-2]]
    if nagy_latest > nagy_previous:
        context.buysignal = 1
    else:
        context.buysignal = 0
    print "done with before_trading_start"

TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'market_config_location': market_config_location,
                     'fundamental_folder': fundamental_folder,
                     'announcement_date_file': announcement_date_file,
                     'data_frequency': data_frequency,
                     'emission_rate': emission_rate,
                     'period_start': period_start,
                     'period_end': period_end,
                     'before_trading_start': before_trading_start,
                     'capital_base': 200000,
                     'warming_period':1,
                     }       
algo = zp.TradingAlgorithm(**TradingDictionary)
#whether to use the data feed to overwrite the trading dictionary
results = algo.run(data, overwrite_sim_params = False)
