# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 21:36:11 2016

@author: Toby
"""
import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china1.3")
import pandas as pd
import numpy as np
import datetime
import pytz

from zipline.api import(
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
)

import zipline as zp
from zipline.dailydata import DBProxy
from zipline.analysis import resolve_orders
from zipline.analysis import plot
period_start = '20141001'
period_end = '20150601'
dbProxy = DBProxy.DBProxy()
cap = dbProxy._get_sn_ts_optional({'TOTMKTCAP': float}, 'tq_qt_skdailyprice', period_start, period_end)
def initialize(context):
    set_long_only()
    """
    限定不能卖空
    """
    context.lookback = 41
    """
    设定半年的交易日数量
    """
    add_history(context.halfyear, '1d', 'ret', ffill=False)
    add_history(1, '1d', 'TOTMKTCAP')
    """
    注册半年长度的股价序列，以便回测时获取这个数据
    """
    add_history(20, '1d', 'volume')
    """
    同上，注册成交量序列，一个月的
    """
    context.histhigh = pd.Series()
    """
    初始化一个历史新高变量，用于存储某个时间所有股票的五日均线半年以来的最高值
    （注意是先取每个窗口的移动平均，再对半年来所有窗口取max）
    """
def handle_data(context, data):
    today = get_datetime()
    if today.date() == datetime.date(2015,7,7):
        print "pause"
    my_orders = get_open_orders()
    """
    当天闭市后第一件事，获得所有挂单（今天没执行完的）

    """
    cap = history(1, '1d', 'TOTMKTCAP')
    cap = cap.iloc[-1,:]
    cap.sort(inplace = True)
    top500 = set(cap.index[-500:])
    ret_history = history(context.lookback, '1d', 'ret', ffill=False)
    """
    得到N只股票半年的行情序列（120个交易日），120行，N列，N是目前所有A股票数
    """
    ret_history[abs(ret_history)>15]=0
    price_history = (1+ret_history/100.0).cumprod(axis=0)
    ma_price_5 = pd.rolling_mean(price_history, 5)
    """
    得到移动平均线，也是120*N，前四天是NA，因为是每只股票五日平均线
    """
    context.histhigh = ma_price_5.max()
    """
    得到这半年来的历史最高移动平均值
    """
    drawdown = (context.histhigh - price_history.iloc[-1,:])/context.histhigh
    """
    得到今天相对历史最高移动平均值的回撤
    """
    filter1 = drawdown > 0.1
    """
    选出回撤大于50%的股票
    """
    vol_history = history(20, '1d', 'volume')
    """
    拿到成交量20个交易日的行情序列，20 by N
    """
    ma_vol_20 = vol_history.mean()
    """
    近一个月的平均成交量
    """
    today_vol = vol_history.iloc[-1,:]
    """
    今天的成交量
    """
    priorday_vol = vol_history.iloc[-2,:]
    """
    昨天的成交量
    """    
    filter2 = ( today_vol > ma_vol_20*3.0 ) & (priorday_vol > ma_vol_20*3.0)
    """
    选出今天和昨天的成交量都是近一个月成交量的3倍以上的股票
    """
    today_price = price_history.iloc[-1,:]
    """
    今天的收盘价
    """
    priorday_price = price_history.iloc[-2,:]
    """
    作天的收盘价
    """
    previous_price = price_history.iloc[-3,:]
    """
    前天的收盘价
    """
    filter3 = (today_price > previous_price) & (priorday_price > previous_price)
    """
    选出今天和昨天收盘价都大于前天的股票
    """
    filtered = filter1 & filter2 & filter3
    selected = set(filtered[filtered == True].index)
    selected = selected.intersection(top500)

    """
    对以上三个过滤求交集，得到我们选择的股票
    """
    
    sell =  list()
    """
    初始化一个list，放我们要卖的股票
    """
    portfolio = context.portfolio
    """
    获得闭市后的portfolio对象（组合）
    """
    
    for pos in [pos for pos in portfolio.positions.itervalues() if pos.amount >= 100]:
        if pos.last_sale_price >= 1.8*pos.cost_basis or pos.last_sale_price <= 0.9*pos.cost_basis:
            sell.append(pos.sid) 
    """
    对所有的头寸，检验最近的收盘价是否超过平均购买成本的1.8倍或低于0.9倍，若是则加入卖出的列表
    """ 
                
    sell = set(sell)
    """
    把列表转化为集合，便于之后求并交
    """

    
    existing_sids = set([sid for sid, pos in portfolio.positions.iteritems() if pos.amount >= 100])
    """
    得到目前portfolio里的所有有头寸的股票
    """
    
    remaining = existing_sids.difference(sell)
    """
    得到本来就在portfolio中我们想要继续持有的股票
    """
    
    new = selected.difference(remaining)
    """
    得到需要新加入portfolio的股票
    """
    
    N = len(new) + len(remaining)
    """
    计算预想的新的portfolio含有的股票数量
    """
    
    for sid in sell:
        order_target_value(sid, 0.0)  
        """
        先卖出想要卖出的股票
        """
        
    for sid in remaining.union(new):
        order_target_percent(sid, 1.0/N)
        """
        使用order_target_percent命令让我们新的portfolio里的股票等权重（该卖的卖该买的买）
        """

period_start = '20141001'
period_end = '20160101'
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '000300.SH',
                     'warming_period':60,
                     'capital_base': 10000000
                     }           
"""
设定回测参数
warming_period这里设的大一点，因为我们需要的history有半年120个交易日长度，否则太多NA，结果不可靠
"""       
     
algo = zp.TradingAlgorithm(**TradingDictionary)
"""
生成一个回测器对象
"""

results = algo.run(False, cap)   
"""
运行！结果返回到results中
"""
benchmk_returns = algo.perf_tracker.cumulative_risk_metrics.benchmark_returns
algo_returns = algo.perf_tracker.cumulative_risk_metrics.algorithm_returns
dates = algo_returns.index
path = r'E:\Anaconda\Lib\site-packages\zipline_china1.3\zipline\examples\reverse_orders.xlsx'
order_df = resolve_orders.resolve_orders(path, results)
plot.plot(dates, algo_returns, benchmk_returns)

def drawdown(xs):
    dd = np.max(np.maximum.accumulate(xs) - xs) # drawdown for each point in time
    drawdown = np.max(dd) # max drawdown
    return drawdown
    
