# -*- coding: utf-8 -*-
"""
Created on Thu Nov 03 21:46:36 2016

@author: Toby
"""
import sys
import pandas as pd
import numpy as np
import datetime
import pytz
sys.path.insert(0, r"E:")
from zipline.finance.slippage import FreeTradeSlippage

from zipline.api import(
history,
add_history,
set_slippage,
set_long_only,
get_datetime,
get_universe,
cancel_order,
order_target_percent,
order_target_value,
record,
get_open_orders,
)

import zipline as zp

def initialize(context):
    set_slippage(FreeTradeSlippage())
    context.target = target # 设置计算信号标的：沪深300指数
    context.security = security # 设置股票池
    # context.security = ["159919.XSHE","510300.XSHG"] # 设置股票池
    # 159919.XSHE：嘉实沪深300ETF；510300.XSHG：华泰柏瑞沪深300ETF
    # 设置短期和长期均线天数已经各自的天数间隔
    add_history(30, '1d', 'price')
    context.long_days = long_days 
    context.long_daydelta = long_daydelta
    context.short_days = short_days
    context.short_daydelta = short_daydelta 

'''
================================================================================
每天交易时
================================================================================
'''
def handle_data(context, data):
    # 设置每日股票池
    #cap = history(1, '1d', 'TOTMKTCAP')
    #cap = cap.iloc[-1,:]
    #cap.sort(inplace = True)
    #top10 = set(cap.index[-10:])
    #context.security = top10 # 设置股票池为每日市值最大的 10 支股票
    N = len(context.security) # 持仓数目
    
    portfolio = context.portfolio
    toSell = signal_stock_sell(context, data)
    toBuy = signal_stock_buy(context, data)

    # 卖出所有头寸以腾出资金
    if toSell == 1:
        for pos in [pos for pos in portfolio.positions.itervalues() if pos.amount >= 100]:
            order_target_value(pos.sid, 0)
    # 执行买入操作
    if toBuy == 1:
        for sid in context.security:
            if sid not in portfolio.positions:
                value = portfolio.cash / N       
                order_target_value(sid, value) 
    # if not toBuy or toSell:
        # locontext.info("今日无操作")
        # send_message("今日无操作")

#获得卖出信号
#输入：context, data
#输出：sell - list
def signal_stock_sell(context, data):
    sell = 0
    # 算出今天和昨天的两个移动均线的值
    (ma_long_pre,ma_long_now) = get_MA(context.long_days, context.long_daydelta)
    (ma_short_pre,ma_short_now) = get_MA(context.short_days, context.short_daydelta)
    # 如果短均线从上往下穿越长均线，则为死叉信号，标记卖出
    if ma_long_now < ma_long_pre and ma_short_now < ma_long_now and ma_short_pre > ma_long_pre:
    #if ma_short_now < ma_long_now and ma_short_pre > ma_long_pre:
    # and context.portfolio.positions[context.security[i]].sellable_amount > 0:
        sell = 1
    return sell

#获得买入信号
#输入：context, data
#输出：buy - list
def signal_stock_buy(context, data):
    buy = 0
    # 算出今天和昨天的两个移动均线的值，
    (ma_long_pre,ma_long_now) = get_MA(context.long_days, context.long_daydelta)
    (ma_short_pre,ma_short_now) = get_MA(context.short_days, context.short_daydelta)
    # 如果短均线从下往上穿越长均线，则为金叉信号，标记买入
    if ma_short_pre < ma_short_now and  ma_short_now > ma_long_now and ma_short_pre < ma_long_pre:
    #if ma_short_now > ma_long_now and ma_short_pre < ma_long_pre:
    # and context.portfolio.positions[context.security[i]].sellable_amount == 0 :
        buy = 1
    return buy

# 计算移动平均线数据
# 输入：股票代码-字符串，移动平均线天数-整数
# 输出：算术平均值-浮点数
def get_MA(days, daydelta):
    # 获得前days天的数据，详见API
    a = history(days+1, '1d', 'price')[target]
    # 定义一个局部变量sum，用于求和
    sum_now = 0
    sum_pre = 0
    # 对前days天每隔daydelta天的收盘价进行求和
    for i in range(1, days+1, daydelta):
        sum_now += a.iloc[-i]
    for i in range(2, days+2, daydelta):
        sum_pre += a.iloc[-i]
    # 求和之后除以天数就可以的得到算术平均值啦
    return (sum_pre/days*daydelta, sum_now/days*daydelta)

'''
================================================================================
每天收盘后
================================================================================
'''
# 每日收盘后要做的事情（本策略中不需要）
def after_trading_end(context):
    return

'''
================================================================================
设定回测参数
================================================================================
'''
# 设置策略参数
# 设置趋势标的及股票池
target = '2070000060'
security = ['2070000103'] # 设置股票池
# 短期和长期均线天数已经各自的天数间隔
long_days = 20 
long_daydelta = 1
short_days = 1
short_daydelta = 1 
#
period_start = '20140701'
period_end = '20151231'
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '2070000060',
                     'warming_period': 20,
                     'capital_base': 16000000,
                     'security_type': 'index',
                     } 
     
'''
================================================================================
生成一个回测器对象
================================================================================
'''   

algo = zp.TradingAlgorithm(**TradingDictionary)
results = algo.run(False)  