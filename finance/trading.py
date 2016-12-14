#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import bisect
import logbook
import datetime

import pandas as pd
import numpy as np

from zipline.data.loader import load_market_data, load_treasury, load_benchmark
from zipline.utils import tradingcalendar
from zipline.dailydata import DBProxy
log = logbook.Logger('Trading')
dbProxy = DBProxy.DBProxy()


# The financial simulations in zipline depend on information
# about the benchmark index and the risk free rates of return.
# The benchmark index defines the benchmark returns used in
# the calculation of performance metrics such as alpha/beta. Many
# components, including risk, performance, transforms, and
# batch_transforms, need access to a calendar of trading days and
# market hours. The TradingEnvironment maintains two time keeping
# facilities:
#   - a DatetimeIndex of trading days for calendar calculations
#   - a timezone name, which should be local to the exchange
#   hosting the benchmark index. All dates are normalized to UTC
#   for serialization and storage, and the timezone is used to
#   ensure proper rollover through daylight savings and so on.
#
# This module maintains a global variable, environment, which is
# subsequently referenced directly by zipline financial
# components. To set the environment, you can set the property on
# the module directly:
#       from zipline.finance import trading
#       trading.environment = TradingEnvironment()
#
# or if you want to switch the environment for a limited context
# you can use a TradingEnvironment in a with clause:
#       lse = TradingEnvironment(bm_index="^FTSE", exchange_tz="Europe/London")
#       with lse:
# the code here will have lse as the global trading.environment
#           algo.run(start, end)
#
# User code will not normally need to use TradingEnvironment
# directly. If you are extending zipline's core financial
# compponents and need to use the environment, you must import the module
# NOT the variable. If you import the module, you will get a
# reference to the environment at import time, which will prevent
# your code from responding to user code that changes the global
# state.

environment = None


class NoFurtherDataError(Exception):
    """
    Thrown when next trading is attempted at the end of available data.
    """
    pass


class TradingEnvironment(object):
    """
    @classmethod
    def instance(cls):
        global environment
        if not environment:
            environment = TradingEnvironment()

        return environment
    """
    def __init__(
        self,
        data_frequency,
        market_config_location = None,
        secode = 2070000061,
        load=None,
        exchange_tz="US/Eastern",
        max_date=None,
        env_trading_calendar=tradingcalendar
    ):
        self.prev_environment = self
        if market_config_location:
            if data_frequency == 'daily':
                self.bm_file = r"{0}\benchmark_daily.pkl".format(market_config_location)
            else:
                self.bm_file = r"{0}\benchmark_minute.pkl".format(market_config_location)
            self.tr_file = r"{0}\treasuries.pkl".format(market_config_location)
            self.tc_file = r"{0}\TradingCalendar.pkl".format(market_config_location)
            self.benchmark_returns = load_benchmark(self.bm_file)
            self.treasury_curves = load_treasury(self.tr_file)
        elif secode:
            if data_frequency == 'daily':
                self.benchmark_returns, self.treasury_curves = \
                dbProxy._get_market_data(secode)
        if not load:
            load = load_market_data
        
        
        if max_date:
            tr_c = self.treasury_curves
            # Mask the treasury curvers down to the current date.
            # In the case of live trading, the last date in the treasury
            # curves would be the day before the date considered to be
            # 'today'.
            self.treasury_curves = tr_c[tr_c.index <= max_date]

        self.exchange_tz = exchange_tz

        # `tc_td` is short for "trading calendar trading days"
        tc_td = env_trading_calendar.trading_days

        if max_date:
            self.trading_days = tc_td[tc_td <= max_date].copy()
        else:
            self.trading_days = tc_td.copy()

        self.first_trading_day = self.trading_days[0]
        self.last_trading_day = self.trading_days[-1]

        #trading_days/ open_and_closes.index are from 1990-01-02, timestamp type
        self.open_and_closes = env_trading_calendar.open_and_closes.loc[
            self.trading_days]

    def __enter__(self, *args, **kwargs):
        global environment
        self.prev_environment = environment
        environment = self
        # return value here is associated with "as such_and_such" on the
        # with clause.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global environment
        environment = self.prev_environment
        # signal that any exceptions need to be propagated up the
        # stack.
        return False

    def normalize_date(self, test_date):
        test_date = pd.Timestamp(test_date, tz='UTC')
        return pd.tseries.tools.normalize_date(test_date)

    def utc_dt_in_exchange(self, dt):
        return pd.Timestamp(dt).tz_convert(self.exchange_tz)

    def exchange_dt_in_utc(self, dt):
        return pd.Timestamp(dt, tz=self.exchange_tz).tz_convert('UTC')

    def is_market_hours(self, test_date):
        if not self.is_trading_day(test_date):
            return False

        mkt_open, mkt_close = self.get_open_and_close(test_date)
        return test_date >= mkt_open and test_date <= mkt_close

    def is_trading_day(self, test_date):
        dt = self.normalize_date(test_date)
        return (dt in self.trading_days)

    def next_trading_day(self, test_date):
        dt = self.normalize_date(test_date)
        delta = datetime.timedelta(days=1)

        while dt <= self.last_trading_day:
            dt += delta
            if dt in self.trading_days:
                return dt

        return None

    def previous_trading_day(self, test_date):
        dt = self.normalize_date(test_date)
        delta = datetime.timedelta(days=-1)

        while self.first_trading_day < dt:
            dt += delta
            if dt in self.trading_days:
                return dt

        return None
        
    def add_trading_days(self, n, date):
        """
        Adds n trading days to date. If this would fall outside of the
        trading calendar, a NoFurtherDataError is raised.

        :Arguments:
            n : int
                The number of days to add to date, this can be positive or
                negative.
            date : datetime
                The date to add to.

        :Returns:
            new_date : datetime
                n trading days added to date.
        """
        if n == 1:
            return self.next_trading_day(date)
        if n == -1:
            return self.previous_trading_day(date)

        idx = self.get_index(date) + n
        if idx < 0 or idx >= len(self.trading_days):
            raise NoFurtherDataError(
                msg='Cannot add %d days to %s' % (n, date)
            )

        return self.trading_days[idx]
        
    def days_in_range(self, start, end):
        mask = ((self.trading_days >= start) &
                (self.trading_days <= end))
        return self.trading_days[mask]
        
    def opens_in_range(self, start, end):
        return self.open_and_closes.market_open.loc[start:end]

    def closes_in_range(self, start, end):
        return self.open_and_closes.market_close.loc[start:end]
        
    def minutes_for_days_in_range(self, start, end):
        """
        Get all market minutes for the days between start and end, inclusive.
        """
        start_date = self.normalize_date(start)
        end_date = self.normalize_date(end)

        all_minutes = []
        for day in self.days_in_range(start_date, end_date):
            day_minutes = self.market_minutes_for_day(day)
            all_minutes.append(day_minutes)

        # Concatenate all minutes and truncate minutes before start/after end.
        return pd.DatetimeIndex(
            np.concatenate(all_minutes), copy=False, tz='UTC',
        )

    def next_open_and_close(self, start_date):
        """
        Given the start_date, returns the next open and close of
        the market.
        """
        next_open = self.next_trading_day(start_date)

        if next_open is None:
            raise NoFurtherDataError(
                "Attempt to backtest beyond available history. \
Last successful date: %s" % self.last_trading_day)

        return self.get_open_and_close(next_open)

    def previous_open_and_close(self, start_date):
        """
        Given the start_date, returns the previous open and close of the
        market.
        """
        previous = self.previous_trading_day(start_date)

        if previous is None:
            raise NoFurtherDataError(
                "Attempt to backtest beyond available history. "
                "First successful date: %s" % self.first_trading_day)
        return self.get_open_and_close(previous)

    def next_market_minute(self, start):
        """
        Get the next market minute after @start. This is either the immediate
        next minute, the open of the same day if @start is before the market
        open on a trading day, or the open of the next market day after @start.
        """
        if self.is_trading_day(start):
            market_open, market_close = self.get_open_and_close(start)
            # If start before market open on a trading day, return market open.
            if start < market_open:
                return market_open
            # If start is during trading hours, then get the next minute.
            elif start < market_close:
                return start + datetime.timedelta(minutes=1)
        # If start is not in a trading day, or is after the market close
        # then return the open of the *next* trading day.
        return self.next_open_and_close(start)[0]

    def previous_market_minute(self, start):
        """
        Get the next market minute before @start. This is either the immediate
        previous minute, the close of the same day if @start is after the close
        on a trading day, or the close of the market day before @start.
        """
        if self.is_trading_day(start):
            market_open, market_close = self.get_open_and_close(start)
            # If start after the market close, return market close.
            if start > market_close:
                return market_close
            # If start is during trading hours, then get previous minute.
            if start > market_open:
                return start - datetime.timedelta(minutes=1)
        # If start is not a trading day, or is before the market open
        # then return the close of the *previous* trading day.
        return self.previous_open_and_close(start)[1]

    def get_open_and_close(self, day):
        todays_minutes = self.open_and_closes.loc[day.date()]

        return todays_minutes['market_open'], todays_minutes['market_close']

    def market_minutes_for_day(self, stamp):
        market_open, market_close = self.get_open_and_close(stamp)
        return pd.date_range(market_open, market_close, freq='T')

    def open_close_window(self, start, count, offset=0, step=1):
        """
        Return a DataFrame containing `count` market opens and closes,
        beginning with `start` + `offset` days and continuing `step` minutes at
        a time.
        """
        # TODO: Correctly handle end of data.
        start_idx = self.get_index(start) + offset
        stop_idx = start_idx + (count * step)

        index = np.arange(start_idx, stop_idx, step)

        return self.open_and_closes.iloc[index]

    def market_minute_window(self, start, count, step=1):
        """
        Return a DatetimeIndex containing `count` market minutes, starting with
        `start` and continuing `step` minutes at a time.
        """
        if not self.is_market_hours(start):
            raise ValueError("market_minute_window starting at "
                             "non-market time {minute}".format(minute=start))

        all_minutes = []

        current_day_minutes = self.market_minutes_for_day(start)
        first_minute_idx = current_day_minutes.searchsorted(start)
        minutes_in_range = current_day_minutes[first_minute_idx::step]

        # Build up list of lists of days' market minutes until we have count
        # minutes stored altogether.
        while True:

            if len(minutes_in_range) >= count:
                # Truncate off extra minutes
                minutes_in_range = minutes_in_range[:count]

            all_minutes.append(minutes_in_range)
            count -= len(minutes_in_range)
            if count <= 0:
                break

            if step > 0:
                start, _ = self.next_open_and_close(start)
                current_day_minutes = self.market_minutes_for_day(start)
            else:
                _, start = self.previous_open_and_close(start)
                current_day_minutes = self.market_minutes_for_day(start)

            minutes_in_range = current_day_minutes[::step]

        # Concatenate all the accumulated minutes.
        return pd.DatetimeIndex(
            np.concatenate(all_minutes), copy=False, tz='UTC',
        )

    def trading_day_distance(self, first_date, second_date):
        first_date = self.normalize_date(first_date)
        second_date = self.normalize_date(second_date)

        # TODO: May be able to replace the following with searchsorted.
        # Find leftmost item greater than or equal to day
        i = bisect.bisect_left(self.trading_days, first_date)
        if i == len(self.trading_days):  # nothing found
            return None
        j = bisect.bisect_left(self.trading_days, second_date)
        if j == len(self.trading_days):
            return None

        return j - i

    def get_index(self, dt):
        """
        Return the index of the given @dt, or the index of the preceding
        trading day if the given dt is not in the trading calendar.
        """
        ndt = self.normalize_date(dt)
        if ndt in self.trading_days:
            return self.trading_days.searchsorted(ndt)
        else:
            return self.trading_days.searchsorted(ndt) - 1


class SimulationParameters(object):
    def __init__(self, period_start, period_end,
                 warming_period = None,
                 capital_base = 10e3,
                 sids = None,
                 data_frequency ='daily',
                 emission_rate ='daily',         
                 env = None,):

        self.period_start = period_start
        self.period_end = period_end
        self.warming_period = warming_period
        self.capital_base = capital_base
        self.sids = sids
        self.active_sids = None
        self.data_frequency = data_frequency
        self.emission_rate = emission_rate        
               
        if env:
            self._update_internal(env)

    def _update_internal(self, env):
        # This is the global environment for trading simulation.

        assert self.period_start <= self.period_end, \
            "Period start falls after period end."

        assert self.period_start <= env.last_trading_day, \
            "Period start falls after the last known trading day."
        assert self.period_end >= env.first_trading_day, \
            "Period end falls before the first known trading day."

        self.first_open = self.calculate_first_open(env)
        self.last_close = self.calculate_last_close(env)
        self.real_open = self.calculate_real_open(env)
        start_index = \
            env.get_index(self.first_open)
        end_index = env.get_index(self.last_close)

        # take an inclusive slice of the environment's
        # trading_days.
        self.trading_days = \
            env.trading_days[start_index:end_index + 1]
            

    def calculate_first_open(self, env):
        """
        Finds the first trading day on or after self.period_start.
        """
        first_open = self.period_start
        one_day = datetime.timedelta(days=1)

        while not env.is_trading_day(first_open):
            first_open = first_open + one_day
        mkt_open = first_open
        if self.data_frequency == 'minute':
            mkt_open, _ = env.get_open_and_close(first_open)
        return mkt_open

    def calculate_last_close(self, env):
        """
        Finds the last trading day on or before self.period_end
        """
        last_close = self.period_end
        one_day = datetime.timedelta(days=1)

        while not env.is_trading_day(last_close):
            last_close = last_close - one_day
        mkt_close = last_close
        if self.data_frequency == 'minute':
            _, mkt_close = env.get_open_and_close(last_close)
        return mkt_close
        
            
    def calculate_real_open(self, env):
        if self.warming_period is None or self.warming_period is 0:
            return self.calculate_first_open(env)
        else:
            one_day = datetime.timedelta(days=1)
            ndays = 0
            real_open = self.first_open
            while (not env.is_trading_day(real_open)) or (ndays < self.warming_period):
                real_open = real_open - one_day
                ndays += 1
            return real_open
            
    @property
    def days_in_period(self):
        """return the number of trading days within the period [start, end)"""
        return len(self.trading_days)

    def __repr__(self):
        return """
{class_name}(
    period_start={period_start},
    period_end={period_end},
    warming_period={warming_period},
    capital_base={capital_base},
    data_frequency={data_frequency},
    emission_rate={emission_rate},
    first_open={first_open},
    last_close={last_close})\
""".format(class_name=self.__class__.__name__,
           period_start=self.period_start,
           period_end=self.period_end,
           warming_period = self.warming_period,
           capital_base=self.capital_base,
           data_frequency=self.data_frequency,
           emission_rate=self.emission_rate,
           first_open=self.first_open,
           last_close=self.last_close)
