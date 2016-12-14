# -*- coding: utf-8 -*-
"""
Created on Wed May 11 20:33:10 2016

@author: Toby
"""

import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china")
import pandas as pd
import datetime
import pickle
import pytz
from zipline.DailyData import dataGen
import time
source = dataGen.dailyData()

#data and config
market_config_location = r'D:\Data\MarketConfig'
market_data_location = r'D:\Data\DailyData'
RegularSourceType = 'pickle'
CustomFileList = [r'D:\Data\DailyData\cap']
CustomSourceType = 'pickle'
datasource = source(CustomSourceType = CustomSourceType, RegularSourceType = RegularSourceType, RegularSourceDirectory = market_data_location,\
              CustomFileList = CustomFileList)
              
#backtest setting"
data_frequency = 'daily'
emission_rate = 'daily'
period_start = pytz.utc.localize(datetime.datetime.strptime("2006/01/01",'%Y/%m/%d'))
period_end = pytz.utc.localize(datetime.datetime.strptime("2015/01/01",'%Y/%m/%d'))



from zipline.api import (
    add_history,
    add_fundamental,
    history,
    get_fundamentals,
    order_target_percent,
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
    add_history(10, '1d', 'price')
    add_history(20, '1d', 'price')
    context.timespent1 = []
    context.timespent2 = []
    context.touchlimit = 0
    context.old_status = None
    set_slippage(MySlippage(0.1, 0.0))
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
        if remaining_volume < 1:
            # we can't fill any more transactions
            return

        # the current order amount will be the min of the
        # volume available in the bar or the open amount.
        cur_volume = int(min(remaining_volume, abs(order.open_amount)))

        if cur_volume < 1:
            return

        # tally the current amount into our total amount ordered.
        # total amount will be used to calculate price impact
        total_volume = self.volume_for_bar + cur_volume

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
    short_history = history(10, '1d', 'price')
    #t1 = time.clock()
    short_mavg = short_history.mean()
    #t2 = time.clock()
    long_mavg = history(20, '1d', 'price').mean()
    #t3 = time.clock()
    #t = t2 - t1
    #print "short_mavg time: %s"%t
    #context.timespent1.append(t)
    #t = t3 - t2
    #print "long_mavg time: %s"%t
    #context.timespent2.append(t)
    sid_universe = data.keys()
    #sym = symbol('600581.SH')
    # Trading logic
    
    last_price = short_history.iloc[-1]
    prior_day_price = short_history.iloc[-2]
    last_return = (last_price - prior_day_price) / prior_day_price
    context.touchlimit = (last_return > 0.099) | (last_return < -0.099)
    context.status = short_mavg > long_mavg
    
    for sid in sid_universe:
        if not context.touchlimit[sid]:
            if context.old_status is not None:
                if not context.old_status[sid] and context.status[sid]:
                    order_target_percent(sid, 0.0)
                elif context.old_status[sid] and not context.status[sid]:
                    order_target_percent(sid, 1.0)
    context.old_status = context.status

    # Save values for later inspection
           
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'market_config_location': market_config_location,
                     'data_frequency': data_frequency,
                     'emission_rate': emission_rate,
                     'period_start': period_start,
                     'period_end': period_end,
                     'warming_period':20,
                     'capital_base': 200000,
                     }         
                     
algo = zp.TradingAlgorithm(**TradingDictionary)
#do not use source period start and period end
results = algo.run(datasource, overwrite_sim_params = False)

with open(r'D:\Data\RunResults\MA Strategy\600581SH', 'w') as f:
    pickle.dump([algo.sim_params, algo.blotter.orders, algo.perf_tracker.perf_periods, algo.perf_tracker.cumulative_risk_metrics], f)