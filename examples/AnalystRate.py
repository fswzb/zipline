# -*- coding: utf-8 -*-
"""
Created on Sat Jul 16 13:52:28 2016

@author: Toby
"""

# -*- coding: utf-8 -*-


import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china")
import pandas as pd
import datetime
import pickle
import pytz
from zipline.dailydata import dataGen
import time
import math
import numpy as np
source = dataGen.dailyData()

#data and config
market_config_location = r'E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\MarketConfig'
market_data_location = r'E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\pickle'
RegularSourceType = 'pickle'
CustomFileList = [r'E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\pickle\cap']
CustomSourceType = 'pickle'
datasource = source(CustomSourceType = CustomSourceType, RegularSourceType = RegularSourceType, RegularSourceDirectory = market_data_location,\
              CustomFileList = CustomFileList)
analystrate = pd.read_pickle(r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\EventData\Analyst Prediction\analyst_rating2.pkl")  
analystrate = analystrate[~np.isnan(analystrate['rating_chg'])]            
#backtest setting"
data_frequency = 'daily'
emission_rate = 'daily'
period_start = pytz.utc.localize(datetime.datetime.strptime("2006/1/01",'%Y/%m/%d'))
period_end = pytz.utc.localize(datetime.datetime.strptime("2016/04/01",'%Y/%m/%d'))


from zipline.api import (
    add_history,
    add_fundamental,
    history,
    get_fundamentals,
    order_target_percent,
    cancel_order,
    record,
    symbol,
    set_slippage,
    set_commission
)
import zipline as zp
import math
from zipline.finance.slippage import SlippageModel, create_transaction
from zipline.finance.commission import PerDollar


def initialize(context):
    # Register 2 histories that track daily prices,
    # one with a 100 window and one with a 300 day window
    add_history(1,'1d','cap')
    context.old_status = None
    context.timespent1 = []
    context.timespent2 = []
    context.touchlimit = 0
    set_slippage(MySlippage(0.1, 0.1))
    set_commission(PerDollar())

  
class MySlippage(SlippageModel):

    def __init__(self, volume_limit=0.1, price_impact=0.0):

        self.volume_limit = volume_limit
        self.price_impact = price_impact

    def __repr__(self):
        return """
{class_name}(
    volume_limit={volume_limit},
    price_impact={price_impact})
""".strip().format(class_name=self.__class__.__name__,
                   volume_limit=self.volume_limit,
                   price_impact=self.price_impact)

    def process_order(self, event, order):

        max_volume = self.volume_limit * event.volume

        # price impact accounts for the total volume of transactions
        # created against the current minute bar
        remaining_volume = max_volume - self.volume_for_bar
        if remaining_volume <= 100:
            # we can't fill any more transactions
            return

        # the current order amount will be the min of the
        # volume available in the bar or the open amount.
        # volume needs to be the multiple of 100
        cur_volume = int(min(math.floor(remaining_volume/100)*100, abs(order.open_amount)))

        if cur_volume < 100:
            return

        # tally the current amount into our total amount ordered.
        # total amount will be used to calculate price impact
        total_volume = self.volume_for_bar + cur_volume
        # volume_share is the percentage of our volume of trade in the total volume for that security
        volume_share = min(total_volume / event.volume,
                           self.volume_limit)

        simulated_impact = volume_share ** 2 \
            * math.copysign(self.price_impact, order.direction) \
            * event.price

        return create_transaction(
            event,
            order,
            # In the future, we may want to change the next line
            # for limit pricing
            event.price + simulated_impact,
            math.copysign(cur_volume, order.direction)
        )
def handle_data(context, data):
    # Skip first 300 days to get full windows

    # Compute averages
    # history() has to be called with the same params
    # from above and returns a pandas dataframe.
    print "time: %s"%context.datetime
    if context.datetime.weekday() in {5,6}:
        return   
    context.selected_sids = None
    cap = history(1, '1d', 'cap')
    cap = cap.iloc[0]
    cap.sort()
    big_caps = cap.iloc[-500:]
    context.preselected = list(big_caps.index)
    if not hasattr(context, 'selected'):
        context.selected = []
    #print context.selected
    #big_caps_prices = prices[context.preselected]
    if not hasattr(context, 'signal_time'):
        context.signal_time = {}
    for sym in data:
        if 'rating_chg' in data[sym] and data[sym]['eventdate'] == context.datetime:    
            rating_chg = data[sym]['rating_chg']
            if not isinstance(rating_chg, float):
                continue
            
            if rating_chg > 0.0:
                if sym in context.selected:
                    context.selected.pop(context.selected.index(sym))
                    order_target_percent(sym, 0.0)
                context.signal_time[sym] = context.datetime
                    
            elif rating_chg < 0.0:
                if sym not in context.selected:
                    context.selected.append(sym)
                context.signal_time[sym] = context.datetime
                    
    N = len(context.selected)
    for sym in context.selected:
        order_target_percent(sym, 1.0/N)
        record(rating_chg = data[sym]['rating_chg'])
        
    current_positions = context.portfolio.positions
    for sym in current_positions:
        if context.signal_time[sym] < context.datetime - datetime.timedelta(20) and sym in context.selected:
            order_target_percent(sym, 0.0)
            context.selected.pop(context.selected.index(sym))
        if sym not in context.selected:
            order_target_percent(sym, 0.0)
    # Save values for later inspection

TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'market_config_location': market_config_location,
                     'data_frequency': data_frequency,
                     'emission_rate': emission_rate,
                     'period_start': period_start,
                     'period_end': period_end,
                     'warming_period':10,
                     }           
            
algo = zp.TradingAlgorithm(**TradingDictionary)
#do not use source period start and period end
results = algo.run(datasource, overwrite_sim_params = False, optional_source = [analystrate])

#with open(r'D:\Data\RunResults\SmallCap030', 'w') as f:
    #pickle.dump([algo.sim_params, algo.blotter.orders, algo.perf_tracker.perf_periods, algo.perf_tracker.cumulative_risk_metrics], f)