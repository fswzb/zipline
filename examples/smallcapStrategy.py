# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 20:35:03 2016

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
period_start = pytz.utc.localize(datetime.datetime.strptime("2005/01/01",'%Y/%m/%d'))
period_end = pytz.utc.localize(datetime.datetime.strptime("2015/01/01",'%Y/%m/%d'))


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
    add_history(101, '1d', 'price')
    add_history(1,'1d','cap')
    context.old_status = None
    context.timespent1 = []
    context.timespent2 = []
    context.touchlimit = 0
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
    prices = history(101, '1d', 'price')
    index = symbol('000001.SH')
    index_return = (prices[index].iloc[100] - prices[index].iloc[0])/prices[index].iloc[0] 
    if index_return > -0.03:
        
        context.selected_sids = None
        cap = history(1, '1d', 'cap')
        cap = cap.iloc[0]
        cap.sort()
        small_caps = cap.iloc[:50]
        context.selected_sids = list(small_caps.index)
    
        small_caps_prices = prices[context.selected_sids]
        returns = (small_caps_prices.iloc[100] - small_caps_prices.iloc[0])/small_caps_prices.iloc[0]
        with_low_returns = list(returns.index[returns < index_return - 0.3])
        context.selected = with_low_returns
    
        context.N = len(context.selected)
        for sym in context.selected:
            order_target_percent(sym, 1.0/context.N)
            record(cap = cap.ix[sym], returns = returns.ix[sym])
        existing_orders = context.get_open_orders()
        #cancel open orders of those not selected
        for sym, orders_sym in existing_orders.iteritems():
            if sym not in context.selected:
                for order in orders_sym:
                    cancel_order(order)
        #clear positions of those not selected
        current_positions = context.portfolio.positions
        for sym in current_positions:
            if sym not in context.selected:
                order_target_percent(sym, 0.0)

    else:
        existing_orders = context.get_open_orders()
        for sym, orders_sym in existing_orders.iteritems():
            for order in orders_sym:
                cancel_order(order)
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
results = algo.run(datasource, overwrite_sim_params = False)

with open(r'D:\Data\RunResults\SmallCap030', 'w') as f:
    pickle.dump([algo.sim_params, algo.blotter.orders, algo.perf_tracker.perf_periods, algo.perf_tracker.cumulative_risk_metrics], f)