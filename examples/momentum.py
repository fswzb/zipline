# -*- coding: utf-8 -*-
"""
Created on Thu Sep 01 21:24:55 2016

@author: Toby
"""

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

def initialize(context):
    set_long_only()
    """
    限定不能卖空
    """
    add_history(20, '1d', 'price')
    """
    注册15个交易日的股价序列，以便回测时获取这个数据
    """
    context.holdtime = dict()

def drawdowndf(df):
    dd = (np.maximum.accumulate(df)-df)/np.maximum.accumulate(df)
    drawdown = dd.max(0)
    return drawdown
    
def handle_data(context, data):
    today = get_datetime()
    my_orders = get_open_orders()
    """
    当天闭市后第一件事，获得所有挂单（今天没执行完的）
    """
    for sid in my_orders:
        open_orders_4sid = my_orders[sid]
        for order in open_orders_4sid:
            if order.amount > 0:
                cancel_order(order)
                
    price_history = history(11, '1d', 'price')            
    index_series = price_history['000300.SH']
    indexreturn = index_series.pct_change()
    signal = (indexreturn[-1] > 0.05) or (indexreturn[-1] >= indexreturn[-10:].max(0))
    print signal
    if signal:           

        ret = price_history.pct_change()
        """
        得到每日收益率
        """
        ma5ret = pd.rolling_apply(ret,5,lambda x: np.prod(1 + x) - 1)
        ma3ret = pd.rolling_mean(ret, 3)
        
        filter1 = (ma5ret>=0.3).any()
        
        filter2 = (ma3ret>=0.095).any()
        
        filter3 = drawdowndf(price_history) >= 0.15
    
        filtered = (filter1 | filter2) & filter3
        selected = set(filtered[filtered == True].index)
    else:
        selected = set()
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

    existing_sids = set([sid for sid, pos in portfolio.positions.iteritems() if pos.amount >= 100])
    existing_pos = set([pos for sid, pos in portfolio.positions.iteritems() if pos.amount >= 100])
    """
    得到目前portfolio里的所有有头寸的股票
    """
    for sid in existing_sids:
        if sid not in context.holdtime:
            context.holdtime[sid] = 1
        else:
            context.holdtime[sid] += 1
    for pos in existing_pos:
        if pos.last_sale_price <= 0.95*pos.cost_basis:
            sell.append(pos.sid) 
    """
    对所有的头寸，检验最近的收盘价是否超过平均购买成本的1.8倍或低于0.9倍，若是则加入卖出的列表
    """    
    for sid in context.holdtime:
        if context.holdtime[sid]>=3:
            sell.append(sid)    
    sell = set(sell)
    """
    把列表转化为集合（同时删除重复的股票代码），便于之后求并交
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

period_start = '20140101'
period_end = '20160630'
TradingDictionary = {'initialize' : initialize,
                     'handle_data' : handle_data,
                     'period_start': period_start,
                     'period_end': period_end,
                     'benchmark': '000300.SH',
                     'warming_period':21,
                     'capital_base': 16000000
                     }           
"""
设定回测参数
warming_period这里设的大一点，因为我们需要的history有半年120个交易日长度，否则太多NA，结果不可靠
"""       
     
algo = zp.TradingAlgorithm(**TradingDictionary)
"""
生成一个回测器对象
"""

results = algo.run(False)   
"""
运行！结果返回到results中
"""


def drawdown(xs):
    dd = np.max((np.maximum.accumulate(xs) - xs)/np.maximum.accumulate(xs)) # drawdown for each point in time
    drawdown = np.max(dd) # max drawdown
    return drawdown
    
mapping = pd.read_csv(r"D:\Data\mapping\secode.csv")
time = list()
sids = list()
amount = list()
commission = list()
status = list()

for orders in results.orders:
    if orders:
        for o in orders:
            if o['status'] is 1:
                time.append(o['created'])
                sids.append(o['sid'])
                amount.append(o['amount'])
                commission.append(o['commission'])
                status.append(o['status'])
            
order_df = pd.DataFrame({'time':time,'sid':sids,'amount':amount,'commission':commission, 'status':status})
mapping.columns=['sid','symbol']
mapping.index=mapping.sid 
order_df['symbol'] = nan
for idx, row in order_df.iterrows():
    sid = row['sid']
    symbol = mapping.ix[int(sid)].symbol
    l = len(str(symbol))
    if l<6:
        symbol = (6-l)*'0'+str(symbol)
    order_df.ix[idx, 'symbol'] = symbol
order_df['time'] = order_df['time'].apply(lambda x:x.strftime(r"%Y/%m/%d"))
order_df.to_excel(r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\examples\order_analysis.xlsx",sheet_name='sheet1')
