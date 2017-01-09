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
from copy import copy
import pytz
import pandas as pd
import numpy as np

from datetime import datetime
from itertools import groupby, chain
from six.moves import filter
from six import iteritems, exec_
from operator import attrgetter

from zipline.errors import (
    OrderDuringInitialize,
    OverrideCommissionPostInit,
    OverrideSlippagePostInit,
    RegisterTradingControlPostInit,
    UnsupportedCommissionModel,
    UnsupportedOrderParameters,
    UnsupportedSlippageModel,
)

from zipline.finance.trading import TradingEnvironment
from zipline.finance.blotter import Blotter
from zipline.finance.commission import PerShare, PerTrade, PerDollar, PerDollar_A
from zipline.finance.controls import (
    LongOnly,
    MaxOrderCount,
    MaxOrderSize,
    MaxPositionSize,
)
from zipline.finance.execution import (
    LimitOrder,
    MarketOrder,
    StopLimitOrder,
    StopOrder,
)
from zipline.finance.performance import PerformanceTracker
from zipline.finance.slippage import (
    VolumeShareSlippage,
    SlippageModel,
    MySlippage,
    FreeTradeSlippage,
    transact_partial
)
from zipline.gens.composites import (
    date_sorted_sources,
    sequential_transforms,
)
from zipline.gens.tradesimulation import AlgorithmSimulator
from zipline.sources import DataFrameSource
from zipline.transforms.utils import StatefulTransform
from zipline.utils.api_support import ZiplineAPI, api_method
from zipline.utils.factory import create_simulation_parameters

import zipline.protocol
from zipline.protocol import Event

from zipline.history import HistorySpec
from zipline.history.history_container import HistoryContainer
from zipline.fundamentals.fundamentals import Fundamental
from zipline.dailydata import DBProxy
DEFAULT_CAPITAL_BASE = float("1.0e5")
dbProxy = DBProxy.DBProxy()

class TradingAlgorithm(object):
    """
    Base class for trading algorithms. Inherit and overload
    initialize() and handle_data(data).

    A new algorithm could look like this:
    ```
    from zipline.api import order

    def initialize(context):
        context.sid = 'AAPL'
        context.amount = 100

    def handle_data(self, data):
        sid = context.sid
        amount = context.amount
        order(sid, amount)
    ```
    To then to run this algorithm pass these functions to
    TradingAlgorithm:

    my_algo = TradingAlgorithm(initialize, handle_data)
    stats = my_algo.run(data)

    """

    # If this is set to false then it is the responsibility
    # of the overriding subclass to set initialized = true
    AUTO_INITIALIZE = True

    def __init__(self, *args, **kwargs):
        """Initialize sids and other state variables.

        :Arguments:
        :Optional:
            initialize : function
                Function that is called with a single
                argument at the begninning of the simulation.
            handle_data : function
                Function that is called with 2 arguments
                (context and data) on every bar.
            script : str
                Algoscript that contains initialize and
                handle_data function definition.
            data_frequency : str (daily, hourly or minutely)
               The duration of the bars.
            capital_base : float <default: 1.0e5>
               How much capital to start with.
            instant_fill : bool <default: False>
               Whether to fill orders immediately or on next bar.
        """
        self.datetime = None

        self.registered_transforms = {}
        self.transforms = []
        self.sources = []

        # List of trading controls to be used to validate orders.
        self.trading_controls = []

        self._recorded_vars = {}
        self.namespace = kwargs.get('namespace', {})

        self.logger = None

        self.benchmark_return_source = None

        # default components for transact
        self.slippage = MySlippage(0.1,0.0)
        self.commission = PerDollar_A()

        self.instant_fill = kwargs.pop('instant_fill', False)
        
        if 'emission_rate' in kwargs:
            emission_rate = kwargs.pop('emission_rate')
        else:
            emission_rate = 'daily'
            
        if 'data_frequency' in kwargs:
            data_frequency = kwargs.pop('data_frequency')
        else:
            data_frequency ='daily'   
            
        if 'market_config_location' in kwargs:
            market_config_location = kwargs.pop('market_config_location')
        else:
            market_config_location = None
        
        if 'benchmark' in kwargs:
            secode = kwargs.pop('benchmark')
        else:
            secode = None
            
        if 'fundamental_folder' in kwargs:
            self.fundamental_folder = kwargs.pop('fundamental_folder')
        else:
            self.fundamental_folder = None
        
        if 'announcement_date_file' in kwargs:
            self.announcement_date_file = kwargs.pop('announcement_date_file')
        else:
            self.announcement_date_file = None
            
        self.trading_environment = kwargs.pop('env', None)

        if self.trading_environment is None:
            if market_config_location:
                self.trading_environment = TradingEnvironment(data_frequency, market_config_location)
            elif secode:
                self.trading_environment = TradingEnvironment(data_frequency, None, secode)
            else:
                self.trading_environment = TradingEnvironment(data_frequency)
        # set the capital base
        self.capital_base = kwargs.pop('capital_base', DEFAULT_CAPITAL_BASE)
        
        if "period_start" in kwargs:
            period_start = kwargs.pop('period_start')
            self.period_start = period_start
        else:
            self.period_start = None

        if "period_end" in kwargs:
            period_end = kwargs.pop('period_end')
            self.period_end = period_end
        else:
            self.period_end = None 
            
        if "warming_period" in kwargs:
            self.warming_period = kwargs.pop('warming_period')
        else:
            self.warming_period = 0 
        
        if "security_type" in kwargs:
            self.security_type = kwargs.pop('security_type')
        else:
            self.security_type = "stock"
        
        self.sim_params = kwargs.pop('sim_params', None)
        if self.sim_params is None:
            self.sim_params = create_simulation_parameters(
                start = pytz.utc.localize(datetime.strptime(self.period_start,r'%Y%m%d')),
                end = pytz.utc.localize(datetime.strptime(self.period_end,r'%Y%m%d')),
                capital_base = self.capital_base,
                data_frequency = data_frequency,
                emission_rate = emission_rate,         
                warming_period = self.warming_period,
                env = self.trading_environment
            )
        self.perf_tracker = PerformanceTracker(self.sim_params, self.trading_environment)

        self.blotter = kwargs.pop('blotter', None)
        if not self.blotter:
            self.blotter = Blotter()

        self.portfolio_needs_update = True
        self._portfolio = None

        self.history_container_class = kwargs.pop(
            'history_container_class', HistoryContainer,
        )
        self.history_container = None
        self.history_specs = {}
        self.fundamentals = {}

        # If string is passed in, execute and get reference to
        # functions.
        self.algoscript = kwargs.pop('script', None)

        self._initialize = None
        self._before_trading_start = None
        self._analyze = None

        if self.algoscript is not None:
            exec_(self.algoscript, self.namespace)
            self._initialize = self.namespace.get('initialize', None)
            if 'handle_data' not in self.namespace:
                raise ValueError('You must define a handle_data function.')
            else:
                self._handle_data = self.namespace['handle_data']
            self._before_trading_start = \
                self.namespace.get('before_trading_start')

            # Optional analyze function, gets called after run
            self._analyze = self.namespace.get('analyze', None)
            self._get_fundamentals = self.namespace.get('get_fundamentals', None)
            
        elif kwargs.get('initialize', False) and kwargs.get('handle_data'):
            if self.algoscript is not None:
                raise ValueError('You can not set script and \
                initialize/handle_data.')
            self._initialize = kwargs.pop('initialize')
            self._handle_data = kwargs.pop('handle_data')
            self._before_trading_start = kwargs.pop('before_trading_start',
                                                    None)
            self._get_fundamentals = kwargs.pop('get_fundamentals', None)                                        
        # If method not defined, NOOP
        if self._initialize is None:
            self._initialize = lambda x: None
            
            
        if self._get_fundamentals is None:
            self._get_fundamentals = lambda x:None

            
        # Alternative way of setting data_frequency for backwards
        # compatibility.


        # Subclasses that override initialize should only worry about
        # setting self.initialized = True if AUTO_INITIALIZE is
        # is manually set to False.
        self.initialized = False
        self.initialize(*args, **kwargs)
        if self.AUTO_INITIALIZE:
            self.initialized = True

    def initialize(self, *args, **kwargs):
        """
        Call self._initialize with `self` made available to Zipline API
        functions.
        """
        with ZiplineAPI(self):
            self._initialize(self)
    
    def before_trading_start(self, data):
        if self._before_trading_start is None:
            return

        self._before_trading_start(self, data)

    def handle_data(self, data):
        self._most_recent_data = data
        if self.history_container:
            self.history_container.update(self.sim_params.sids, data, self.datetime)

        self._handle_data(self, data)

    def analyze(self, perf):
        if self._analyze is None:
            return

        with ZiplineAPI(self):
            self._analyze(self, perf)

    def __repr__(self):
        """
        N.B. this does not yet represent a string that can be used
        to instantiate an exact copy of an algorithm.

        However, it is getting close, and provides some value as something
        that can be inspected interactively.
        """
        return """
{class_name}(
    capital_base={capital_base}
    sim_params={sim_params},
    initialized={initialized},
    slippage={slippage},
    commission={commission},
    blotter={blotter},
    recorded_vars={recorded_vars})
""".strip().format(class_name=self.__class__.__name__,
                   capital_base=self.capital_base,
                   sim_params=repr(self.sim_params),
                   initialized=self.initialized,
                   slippage=repr(self.slippage),
                   commission=repr(self.commission),
                   blotter=repr(self.blotter),
                   recorded_vars=repr(self.recorded_vars))

    def _create_data_generator(self, source_filter, sim_params=None):
        """
        Create a merged data generator using the sources and
        transforms attached to this algorithm.

        ::source_filter:: is a method that receives events in date
        sorted order, and returns True for those events that should be
        processed by the zipline, and False for those that should be
        skipped.
        """
        if sim_params is None:
            sim_params = self.sim_params

        env = self.trading_environment
        if (sim_params.data_frequency == 'minute' or sim_params.emission_rate == 'minute'):
            update_time = lambda date: date
            #update_time = lambda date: env.get_open_and_close(date)[1]
        else:
            update_time = lambda date: date
        benchmark_return_source = [
        Event({'dt': update_time(dt),
               'returns': ret,
               'type': zipline.protocol.DATASOURCE_TYPE.BENCHMARK,
               'source_id': 'benchmarks'})
        for dt, ret in env.benchmark_returns.iterrows()
            if dt.date() >= sim_params.period_start.date()
            and dt.date() <= sim_params.period_end.date()
            ]


        date_sorted = date_sorted_sources(*self.sources)

        if source_filter:
            date_sorted = filter(source_filter, date_sorted)

        with_tnfms = sequential_transforms(date_sorted,
                                           *self.transforms)

        with_benchmarks = date_sorted_sources(benchmark_return_source,
                                              with_tnfms)

        # Group together events with the same dt field. This depends on the
        # events already being sorted.
        return groupby(with_benchmarks, attrgetter('dt'))

    def _create_generator(self, sim_params, source_filter=None):
        """
        Create a basic generator setup using the sources and
        transforms attached to this algorithm.

        ::source_filter:: is a method that receives events in date
        sorted order, and returns True for those events that should be
        processed by the zipline, and False for those that should be
        skipped.
        """
        # Instantiate perf_tracker
        self.perf_tracker = PerformanceTracker(sim_params, self.trading_environment)
        self.portfolio_needs_update = True

        self.data_gen = self._create_data_generator(source_filter, sim_params)

        self.trading_client = AlgorithmSimulator(self, sim_params)

        transact_method = transact_partial(self.slippage, self.commission)
        self.set_transact(transact_method)

        return self.trading_client.transform(self.data_gen)

    def get_generator(self):
        """
        Override this method to add new logic to the construction
        of the generator. Overrides can use the _create_generator
        method to get a standard construction generator.
        """
        return self._create_generator(self.sim_params)

    # TODO: make a new subclass, e.g. BatchAlgorithm, and move
    # the run method to the subclass, and refactor to put the
    # generator creation logic into get_generator.
    def run(self, overwrite_sim_params=True, *args, **kwargs):
        """Run the algorithm.

        :Arguments:
            source : can be either:
                     - pandas.DataFrame
                     - zipline source
                     - list of sources

               If pandas.DataFrame is provided, it must have the
               following structure:
               * column names must consist of ints representing the
                 different sids
               * index must be DatetimeIndex
               * array contents should be price info.

        :Returns:
            daily_stats : pandas.DataFrame
              Daily performance metrics such as returns, alpha etc.

        """
        if self.security_type is "stock" or self.security_type is 0:
            sn_ts = dbProxy._get_sn_ts_local(self.sim_params.real_open.strftime("%Y%m%d"), self.period_end) 
            delist = dbProxy._get_delist(self.sim_params.real_open.strftime("%Y%m%d"), self.period_end)
            dividend = dbProxy._get_dividends(self.sim_params.real_open.strftime("%Y%m%d"), self.period_end)
            regular_source = DataFrameSource(sn_ts)
            delist_source = DataFrameSource(delist)
            dividends_source = DataFrameSource(dividend)
            del sn_ts
        elif self.security_type is "index" or self.security_type is 1:
            sn_ts = dbProxy._get_index_ts_local(self.sim_params.real_open.strftime("%Y%m%d"), self.period_end) 
            regular_source = DataFrameSource(sn_ts)
        elif self.security_type is "index_futures" or self.security_type is 2:
            sn_ts = dbProxy._get_index_futures(self.sim_params.real_open.strftime("%Y%m%d"), self.period_end) 
            regular_source = DataFrameSource(sn_ts)   
        else:
            print "Please specify proper security types: stock, index, index_futures!!"
            return 0
            
        if 'fundamental_data' in kwargs:
            fundamental_data = kwargs['fundamental_data']
            fundamental_source = map(DataFrameSource, fundamental_data)   
        else:
            fundamental_data = None
            fundamental_source = []
        if args:
            optional_source = []
            for source in args:
                optional_source.append(DataFrameSource(source))
            del args
            if self.security_type is 'stock' or self.security_type is 0:
                self.set_sources([regular_source, delist_source, dividends_source] + optional_source)
            else:
                self.set_sources([regular_source] + optional_source)
        else:
            if self.security_type is 'stock' or self.security_type is 0:
                self.set_sources([regular_source, delist_source, dividends_source])
            else:
                self.set_sources([regular_source])

        #TODO: can probably combine delist_source and regular price source instead of separating them in different events?            
        # Override sim_params if params are provided by the source.
        if overwrite_sim_params:
            if args:
                source = args[0]
                self.sim_params.period_start = source['dt'].ix[0]
                self.sim_params.period_end = source['dt'].ix[-1]
        first_open = self.sim_params.first_open.strftime(r"%Y%m%d")
        if self.security_type is "stock" or self.security_type is 0:
            initial_sids = set(dbProxy._get_sn_ts_local(first_open, first_open)['sid'])
        elif self.security_type is "index" or self.security_type is 1:
            initial_sids = set(dbProxy._get_index_ts_local(first_open, first_open)['sid'])
        elif self.security_type is "index_futures" or self.security_type is 2:
            initial_sids = set(dbProxy._get_index_futures(first_open, first_open)['sid'])
        self.sim_params.sids = initial_sids
        self.sim_params.active_sids = set()
        # Changing period_start and period_close might require updating
        # of first_open and last_close.
        self.sim_params._update_internal(self.trading_environment)
            
        if fundamental_data:
            for f in fundamental_data:
                field = f.columns[-1]
                self.fundamentals[field] = Fundamental(field, self.sim_params.sids, f)   
                
        # Create history containers
        #TODO: history_container seems to have initialized twice
        if len(self.history_specs) != 0:
            self.history_container = self.history_container_class(
                self.history_specs,
                self.sim_params.sids,
                self.sim_params.real_open,
                self.sim_params.data_frequency,
                self.trading_environment,
            )

        # Create transforms by wrapping them into StatefulTransforms
        self.transforms = []
        for namestring, trans_descr in iteritems(self.registered_transforms):
            sf = StatefulTransform(
                trans_descr['class'],
                *trans_descr['args'],
                **trans_descr['kwargs']
            )
            sf.namestring = namestring

            self.transforms.append(sf)

        # force a reset of the performance tracker, in case
        # this is a repeat run of the algorithm.
        self.perf_tracker = None

        # create transforms and zipline
        self.gen = self._create_generator(self.sim_params)

        with ZiplineAPI(self):
            # loop through simulated_trading, each iteration returns a
            # perf dictionary
            perfs = []
            for perf in self.gen:
                perfs.append(perf)

            # convert perf dict to pandas dataframe
            daily_stats = self._create_daily_stats(perfs)

        self.analyze(daily_stats)

        return daily_stats

    def _create_daily_stats(self, perfs):
        # create daily and cumulative stats dataframe
        daily_perfs = []
        # TODO: the loop here could overwrite expected properties
        # of daily_perf. Could potentially raise or log a
        # warning.
        for perf in perfs:
            if 'daily_perf' in perf:

                perf['daily_perf'].update(
                    perf['daily_perf'].pop('recorded_vars')
                )
                daily_perfs.append(perf['daily_perf'])
            else:
                self.risk_report = perf

        daily_dts = [np.datetime64(perf['period_close'], utc=True)
                     for perf in daily_perfs]
        daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)

        return daily_stats

    def add_transform(self, transform_class, tag, *args, **kwargs):
        """Add a single-sid, sequential transform to the model.

        :Arguments:
            transform_class : class
                Which transform to use. E.g. mavg.
            tag : str
                How to name the transform. Can later be access via:
                data[sid].tag()

        Extra args and kwargs will be forwarded to the transform
        instantiation.

        """
        self.registered_transforms[tag] = {'class': transform_class,
                                           'args': args,
                                           'kwargs': kwargs}

    @api_method
    def record(self, *args, **kwargs):
        """
        Track and record local variable (i.e. attributes) each day.
        """
        # Make 2 objects both referencing the same iterator
        args = [iter(args)] * 2

        # Zip generates list entries by calling `next` on each iterator it
        # receives.  In this case the two iterators are the same object, so the
        # call to next on args[0] will also advance args[1], resulting in zip
        # returning (a,b) (c,d) (e,f) rather than (a,a) (b,b) (c,c) etc.
        positionals = zip(*args)
        for name, value in chain(positionals, iteritems(kwargs)):
            self._recorded_vars[name] = value

    @api_method
    def order(self, sid, amount,
              limit_price=None,
              stop_price=None,
              style=None):
        """
        Place an order using the specified parameters.
        """

        def round_to_nearest_100(a):
            return int(a/100.0)*100
        # Truncate to the integer share count that's either within .0001 of
        # amount or closer to zero.
        # E.g. 3.9999 -> 4.0; 5.5 -> 5.0; -5.5 -> -5.0
        #amount = int(round_if_near_integer(amount))
        if not self.sellout:
            amount = round_to_nearest_100(amount)

        # Raises a ZiplineError if invalid parameters are detected.
        valid = self.validate_order_params(sid,
                                           amount,
                                           limit_price,
                                           stop_price,
                                           style)
        if not valid:
            return
        # Convert deprecated limit_price and stop_price parameters to use
        # ExecutionStyle objects.
        style = self.__convert_order_params_for_blotter(limit_price,
                                                        stop_price,
                                                        style)
        #print "algo576: %s amount into blotter.order: %s"%(sid, amount)
        return self.blotter.order(sid, amount, style)

    def validate_order_params(self,
                              sid,
                              amount,
                              limit_price,
                              stop_price,
                              style):
        """
        Helper method for validating parameters to the order API function.

        Raises an UnsupportedOrderParameters if invalid arguments are found.
        """

        if not self.initialized:
            raise OrderDuringInitialize(
                msg="order() can only be called from within handle_data()"
            )

        if style:
            if limit_price:
                raise UnsupportedOrderParameters(
                    msg="Passing both limit_price and style is not supported."
                )

            if stop_price:
                raise UnsupportedOrderParameters(
                    msg="Passing both stop_price and style is not supported."
                )

        for control in self.trading_controls:
            
            if not control.validate(sid,
                                amount,
                                self.updated_portfolio(),
                                self.get_datetime(),
                                self.trading_client.current_data):
                print "cancel order"
                return False
                
        return True

    @staticmethod
    def __convert_order_params_for_blotter(limit_price, stop_price, style):
        """
        Helper method for converting deprecated limit_price and stop_price
        arguments into ExecutionStyle instances.

        This function assumes that either style == None or (limit_price,
        stop_price) == (None, None).
        """
        # TODO_SS: DeprecationWarning for usage of limit_price and stop_price.
        if style:
            assert (limit_price, stop_price) == (None, None)
            return style
        if limit_price and stop_price:
            return StopLimitOrder(limit_price, stop_price)
        if limit_price:
            return LimitOrder(limit_price)
        if stop_price:
            return StopOrder(stop_price)
        else:
            return MarketOrder()

    @api_method
    def order_value(self, sid, value,
                    limit_price=None, stop_price=None, style=None):
        """
        Place an order by desired value rather than desired number of shares.
        If the requested sid is found in the universe, the requested value is
        divided by its price to imply the number of shares to transact.

        value > 0 :: Buy/Cover
        value < 0 :: Sell/Short
        Market order:    order(sid, value)
        Limit order:     order(sid, value, limit_price)
        Stop order:      order(sid, value, None, stop_price)
        StopLimit order: order(sid, value, limit_price, stop_price)
        """
        last_price = self.trading_client.current_data[sid].price
        if np.isnan(last_price) or np.allclose(last_price, 0):
            # Don't place an order
            zero_message = "Price of 0 for {psid}; can't infer value"
            print(zero_message.format(psid=sid))
            return
        else:
            amount = value / last_price
            return self.order(sid, amount,
                              limit_price=limit_price,
                              stop_price=stop_price,
                              style=style)

    @property
    def recorded_vars(self):
        return copy(self._recorded_vars)

    @property
    def portfolio(self):
        return self.updated_portfolio()

    def updated_portfolio(self):
        if self.portfolio_needs_update:
            self._portfolio = self.perf_tracker.get_portfolio()
            self.portfolio_needs_update = False
        return self._portfolio

    def set_logger(self, logger):
        self.logger = logger

    def on_dt_changed(self, dt):
        """
        Callback triggered by the simulation loop whenever the current dt
        changes.

        Any logic that should happen exactly once at the start of each datetime
        group should happen here.
        """
        assert isinstance(dt, datetime), \
            "Attempt to set algorithm's current time with non-datetime"
        assert dt.tzinfo == pytz.utc, \
            "Algorithm expects a utc datetime"

        self.datetime = dt
        self.perf_tracker.set_date(dt)
        self.blotter.set_date(dt)

    @api_method
    def get_datetime(self):
        """
        Returns a copy of the datetime.
        """
        date_copy = copy(self.datetime)
        assert date_copy.tzinfo == pytz.utc, \
            "Algorithm should have a utc datetime"
        return date_copy
    
    @api_method
    def get_universe(self):
        """
        Returns a current snapshot of sids.
        """
        universe_copy = copy(self.sim_params.active_sids)
        return universe_copy
        
    @api_method    
    def get_tradingdates(self):
        """
        Returns the trading dates up to self.datetime.
        """
        tradingdates = copy(self.trading_client.env.trading_days)
        tradingdates_copy = tradingdates[tradingdates <= self.datetime].copy()
        return tradingdates_copy
        
    def set_transact(self, transact):
        """
        Set the method that will be called to create a
        transaction from open orders and trade events.
        """
        self.blotter.transact = transact

    def update_dividends(self, dividend_frame):
        """
        Set DataFrame used to process dividends.  DataFrame columns should
        contain at least the entries in zp.DIVIDEND_FIELDS.
        """
        self.perf_tracker.update_dividends(dividend_frame)

    @api_method
    def set_slippage(self, slippage):
        if not isinstance(slippage, SlippageModel):
            raise UnsupportedSlippageModel()
        if self.initialized:
            raise OverrideSlippagePostInit()
        self.slippage = slippage

    @api_method
    def set_commission(self, commission):
        if not isinstance(commission, (PerShare, PerTrade, PerDollar, PerDollar_A)):
            raise UnsupportedCommissionModel()

        if self.initialized:
            raise OverrideCommissionPostInit()
        self.commission = commission

    def set_sources(self, sources):
        assert isinstance(sources, list)
        self.sources = sources

    def set_transforms(self, transforms):
        assert isinstance(transforms, list)
        self.transforms = transforms

    # Remain backwards compatibility
    @property
    def data_frequency(self):
        return self.sim_params.data_frequency

    @data_frequency.setter
    def data_frequency(self, value):
        assert value in ('daily', 'minute')
        self.sim_params.data_frequency = value
        
    @property
    def emission_rate(self):
        return self.sim_params.emission_rate

    @emission_rate.setter
    def emission_rate(self, value):
        assert value in ('daily', 'minute')
        self.sim_params.emission_rate = value

        
    @api_method
    def order_percent(self, sid, percent,
                      limit_price=None, stop_price=None, style=None):
        """
        Place an order in the specified security corresponding to the given
        percent of the current portfolio value.

        Note that percent must expressed as a decimal (0.50 means 50\%).
        """
        value = self.portfolio.portfolio_value * percent
        return self.order_value(sid, value,
                                limit_price=limit_price,
                                stop_price=stop_price,
                                style=style)

    @api_method
    def order_target(self, sid, target,
                     limit_price=None, stop_price=None, style=None):
        """
        Place an order to adjust a position to a target number of shares. If
        the position doesn't already exist, this is equivalent to placing a new
        order. If the position does exist, this is equivalent to placing an
        order for the difference between the target number of shares and the
        current number of shares.
        """
        if target == 0.0:
            self.sellout = True
        else:
            self.sellout = False
            
        if sid in self.portfolio.positions:
            current_position = self.portfolio.positions[sid].amount
            current_pending_orders = self.get_open_orders(sid)
                #current_pending_position = reduce(lambda x, y: x+y, [(order.amount - order.filled) for order in current_pending_orders])
            for pending_order in current_pending_orders:
                self.cancel_order(pending_order)
                
            req_shares = target - current_position
            return self.order(sid, req_shares,
                              limit_price=limit_price,
                              stop_price=stop_price,
                              style=style)
        else:
            current_pending_orders = self.get_open_orders(sid)
            if current_pending_orders:
                current_pending_position = reduce(lambda x, y: x+y, [(order.amount - order.filled) for order in current_pending_orders])
            else:
                current_pending_position = 0
            req_shares = target - current_pending_position
            return self.order(sid, req_shares,
                              limit_price=limit_price,
                              stop_price=stop_price,
                              style=style)

    @api_method
    def order_target_value(self, sid, target,
                           limit_price=None, stop_price=None, style=None):
        """
        Place an order to adjust a position to a target value. If
        the position doesn't already exist, this is equivalent to placing a new
        order. If the position does exist, this is equivalent to placing an
        order for the difference between the target value and the
        current value.
        """
        if target == 0.0:
            self.sellout = True
        else:
            self.sellout = False
            
        last_price = self.trading_client.current_data[sid].price
        if np.isnan(last_price) or np.allclose(last_price, 0):
            # Don't place an order
            zero_message = "Price of 0 for {psid}; can't infer value"
            print(zero_message.format(psid=sid))
            return
        target_amount = target / last_price
        return self.order_target(sid, target_amount,
                                 limit_price=limit_price,
                                 stop_price=stop_price,
                                 style=style)

    @api_method
    def order_target_percent(self, sid, target,
                             limit_price=None, stop_price=None, style=None):
        """
        Place an order to adjust a position to a target percent of the
        current portfolio value. If the position doesn't already exist, this is
        equivalent to placing a new order. If the position does exist, this is
        equivalent to placing an order for the difference between the target
        percent and the current percent.

        Note that target must expressed as a decimal (0.50 means 50\%).
        """
        if target == 0.0:
            self.sellout = True
        else:
            self.sellout = False
            
        target_value = self.portfolio.portfolio_value * target
        return self.order_target_value(sid, target_value,
                                       limit_price=limit_price,
                                       stop_price=stop_price,
                                       style=style)

    @api_method
    def get_open_orders(self, sid=None):
        if sid is None:
            return {
                key: [order.to_api_obj() for order in orders]
                for key, orders in iteritems(self.blotter.open_orders)
                if orders
            }
        if sid in self.blotter.open_orders:
            orders = self.blotter.open_orders[sid]
            return [order.to_api_obj() for order in orders]
        return []

    @api_method
    def get_order(self, order_id):
        if order_id in self.blotter.orders:
            return self.blotter.orders[order_id].to_api_obj()

    @api_method
    def cancel_order(self, order_param):
        order_id = order_param
        if isinstance(order_param, zipline.protocol.Order):
            order_id = order_param.id

        self.blotter.cancel(order_id)

    def raw_positions(self):
        """
        Returns the current portfolio for the algorithm.

        N.B. this is not done as a property, so that the function can be
        passed and called from within a source.
        """
        # Return the 'internal' positions object, as in the one that is
        # not passed to the algo, and thus should not have tainted keys.
        return self.perf_tracker.cumulative_performance.positions

    def raw_orders(self):
        """
        Returns the current open orders from the blotter.

        N.B. this is not a property, so that the function can be passed
        and called back from within a source.
        """

        return self.blotter.open_orders

    @api_method
    def add_history(self, bar_count, frequency, field, ffill=True):
        data_frequency = self.sim_params.data_frequency
        history_spec = HistorySpec(bar_count, frequency, field, ffill,
                                   data_frequency=data_frequency,
                                   env=self.trading_environment)
        self.history_specs[history_spec.key_str] = history_spec
        if self.initialized:
            if self.history_container:
                self.history_container.ensure_spec(
                    history_spec, self.datetime, self._most_recent_data,
                )
            else:
                self.history_container = self.history_container_class(
                    self.history_specs,
                    self.sim_params.sids,
                    self.sim_params.real_open,
                    self.sim_params.data_frequency,
                    env=self.trading_environment,
                )

    def get_history_spec(self, bar_count, frequency, field, ffill):
        spec_key = HistorySpec.spec_key(bar_count, frequency, field, ffill)
        if spec_key not in self.history_specs:
            data_freq = self.sim_params.data_frequency
            spec = HistorySpec(
                bar_count,
                frequency,
                field,
                ffill,
                data_frequency=data_freq,
                env=self.trading_environment,
            )
            self.history_specs[spec_key] = spec
            if not self.history_container:
                self.history_container = self.history_container_class(
                    self.history_specs,
                    self.sim_params.sids,
                    self.datetime,
                    self.sim_params.data_frequency,
                    bar_data=self._most_recent_data,
                    env=self.trading_environment,
                )
            self.history_container.ensure_spec(
                spec, self.datetime, self._most_recent_data,
            )
        return self.history_specs[spec_key]

    @api_method
    def history(self, bar_count, frequency, field, ffill=True):
        history_spec = self.get_history_spec(
            bar_count,
            frequency,
            field,
            ffill,
        )
        return self.history_container.get_history(history_spec, self.datetime)

    @api_method
    def get_fundamentals(self, field, nlookback = 1):
        fundamental = self.fundamentals[field]
        fundamental._update_universe(self.sim_params.sids)
        return fundamental._get_fundamentals(self.datetime, nlookback)
        

    ####################
    # Trading Controls #
    ####################

    def register_trading_control(self, control):
        """
        Register a new TradingControl to be checked prior to order calls.
        """
        if self.initialized:
            raise RegisterTradingControlPostInit()
        self.trading_controls.append(control)

    @api_method
    def set_max_position_size(self,
                              sid=None,
                              max_shares=None,
                              max_notional=None):
        """
        Set a limit on the number of shares and/or dollar value held for the
        given sid. Limits are treated as absolute values and are enforced at
        the time that the algo attempts to place an order for sid. This means
        that it's possible to end up with more than the max number of shares
        due to splits/dividends, and more than the max notional due to price
        improvement.

        If an algorithm attempts to place an order that would result in
        increasing the absolute value of shares/dollar value exceeding one of
        these limits, raise a TradingControlException.
        """
        control = MaxPositionSize(sid=sid,
                                  max_shares=max_shares,
                                  max_notional=max_notional)
        self.register_trading_control(control)

    @api_method
    def set_max_order_size(self, sid=None, max_shares=None, max_notional=None):
        """
        Set a limit on the number of shares and/or dollar value of any single
        order placed for sid.  Limits are treated as absolute values and are
        enforced at the time that the algo attempts to place an order for sid.

        If an algorithm attempts to place an order that would result in
        exceeding one of these limits, raise a TradingControlException.
        """
        control = MaxOrderSize(sid=sid,
                               max_shares=max_shares,
                               max_notional=max_notional)
        self.register_trading_control(control)

    @api_method
    def set_max_order_count(self, max_count):
        """
        Set a limit on the number of orders that can be placed within the given
        time interval.
        """
        control = MaxOrderCount(max_count)
        self.register_trading_control(control)

    @api_method
    def set_long_only(self):
        """
        Set a rule specifying that this algorithm cannot take short positions.
        """
        self.register_trading_control(LongOnly())

    @classmethod
    def all_api_methods(cls):
        """
        Return a list of all the TradingAlgorithm API methods.
        """
        return [fn for fn in cls.__dict__.itervalues()
                if getattr(fn, 'is_api_method', False)]
