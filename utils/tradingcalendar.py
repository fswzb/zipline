# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 20:54:43 2016

@author: Toby
"""

import pandas as pd
import pytz
import os
from datetime import datetime, timedelta
from functools import partial
from zipline.dailydata import DBProxy


start = pd.Timestamp('1990-01-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
dr = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
csv = os.path.join(dr, r'cache\MarketConfig\tradingdates.csv')
# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end = end_base + timedelta(days=365)
def canonicalize_datetime(dt):
    # Strip out any HHMMSS or timezone info in the user's datetime, so that
    # all the datetimes we return will be 00:00:00 UTC.
    return datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)



def get_trading_days(csv):
    data = pd.read_csv(csv)
    dates = data.Date
    dates = map(lambda x:datetime.strptime(x,"%Y/%m/%d"),dates)
    dates = map(lambda x:pytz.utc.localize(x),dates)
    return pd.DatetimeIndex(dates)
    
#trading_days = get_trading_days(csv)  
dbProxy = DBProxy.DBProxy()
trading_days = dbProxy._get_trading_dates_tmp('20020101', datetime.today().strftime(r"%Y%m%d"))

alldates = pd.date_range(start = trading_days[0].strftime("%m/%d/%Y"), end = trading_days[-1].strftime("%m/%d/%Y"), freq='D',tz='UTC')
def get_non_trading_days(alldates, trading_days):
    droplist = []
    for dt in alldates:
        if dt in trading_days:
            droplist.append(dt)
    return alldates.drop(droplist)
non_trading_days = get_non_trading_days(alldates, trading_days)
trading_day = pd.tseries.offsets.CDay(holidays=non_trading_days)
trading_days = pd.date_range(start = trading_days[0].strftime("%m/%d/%Y"), end = trading_days[-1].strftime("%m/%d/%Y"), freq = trading_day,tz='UTC') 

def get_open_and_close(day, early_closes = []):
    market_open = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=9,
            minute=31),
        tz='UTC')
    # 1 PM if early close, 4 PM otherwise
    close_hour = 13 if day in early_closes else 15
    market_close = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=close_hour),
        tz='UTC')

    return market_open, market_close


def get_open_and_closes(trading_days, early_closes = []):
    open_and_closes = pd.DataFrame(index=trading_days,
                                   columns=('market_open', 'market_close'))

    get_o_and_c = partial(get_open_and_close, early_closes=early_closes)

    open_and_closes['market_open'], open_and_closes['market_close'] = \
        zip(*open_and_closes.index.map(get_o_and_c))

    return open_and_closes

open_and_closes = get_open_and_closes(trading_days)
