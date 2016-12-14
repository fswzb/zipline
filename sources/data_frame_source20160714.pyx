
#
# Copyright 2013 Quantopian, Inc.
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

"""
Tools to generate data sources.
"""
#cython: boundscheck=False
#cython: wraparound=False
import pandas as pd

from zipline.gens.utils import hash_args

from zipline.sources.data_source import DataSource

cimport numpy as cnp
import numpy as np

class DataFrameSource(DataSource):
    """
    Yields all events in event_list that match the given sid_filter.
    If no event_list is specified, generates an internal stream of events
    to filter.  Returns all events if filter is None.

    Configuration options:

    sids   : list of values representing simulated internal sids
    start  : start date
    delta  : timedelta between internal events
    filter : filter to remove the sids
    """

    def __init__(self, data, **kwargs):
        assert isinstance(data.index, pd.tseries.index.DatetimeIndex)

        self.data = data
        # Unpack config dictionary with default values.
        self.sids = kwargs.get('sids', data.columns)
        self.start = kwargs.get('start', data.index[0])
        self.end = kwargs.get('end', data.index[-1])

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(data, **kwargs)

        self._raw_data = None

    @property
    def mapping(self):
        return {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
        }

    @property
    def instance_hash(self):
        return self.arg_string

    def raw_data_gen(self):
        for dt, series in self.data.iterrows():
            for sid, price in series.iterkv():
                if sid in self.sids:
                    event = {
                        'dt': dt,
                        'sid': sid,
                        'price': price,
                        'volume': 1000,
                    }
                    yield event

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data

class DataFrameSingleSource(DataSource):
    def __init__(self, data, **kwargs):
        assert isinstance(data.index, pd.tseries.index.DatetimeIndex)
        
        self.sid = kwargs.get('sid', data.columns[0])
        data.columns = ['price'] + list(data.columns[1:])
        self.data = data
        cdef cnp.ndarray[cnp.float64_t, ndim=2] tempdata = data.values
        self.arraydata = tempdata
        self.dates = data.index
        cdef str [:] tempfields = np.array(data.columns)
        self.fields = tempfields
        # Unpack config dictionary with default values.
        #self.start = kwargs.get('start', data.index[0])
        #self.end = kwargs.get('end', data.index[-1])

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(data, **kwargs)
        self._raw_data = None

    @property
    def mapping(self):
        return {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
        }

    @property
    def instance_hash(self):
        return self.arg_string
        
    def raw_data_gen(self):
        cdef unsigned int dt_idx, field_idx
        cdef str field
        for dt_idx in range(len(self.dates)):
            series = self.arraydata[dt_idx]
            if np.isnan(series).any():
                continue
            event = {'dt': self.dates[dt_idx], 'sid': self.sid}
            for field_idx in range(len(series)):
                field = self.fields[field_idx]
                event[field] = series[field_idx]
            yield event
            

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data
        
class DataPanelSource(DataSource):
    """
    Yields all events in event_list that match the given sid_filter.
    If no event_list is specified, generates an internal stream of events
    to filter.  Returns all events if filter is None.

    Configuration options:

    sids   : list of values representing simulated internal sids
    start  : start date
    delta  : timedelta between internal events
    filter : filter to remove the sids
    """

    def __init__(self, data, **kwargs):
        assert isinstance(data.major_axis, pd.tseries.index.DatetimeIndex)

        self.data = data
        cdef str [:] tempfields = np.array(data.minor_axis)
        self.fields = tempfields
        self.dates = data.major_axis
        cdef str [:] tempsids = np.array(kwargs.get('sids', data.items))
        self.sids = tempsids
        cdef cnp.ndarray[cnp.float64_t, ndim=3] tempdata = data.values.transpose([1,0,2])
        self.arraydata = tempdata
       
        # 0 axis - dates, 1 axis - sids, 2 - fields 
        # Unpack config dictionary with default values.
        
        #self.start = kwargs.get('start', data.major_axis[0])
        #self.end = kwargs.get('end', data.major_axis[-1])

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(data, **kwargs)

        self._raw_data = None

    @property
    def mapping(self):
        mapping = {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'price'),
            'volume': (int, 'volume'),
        }

        # Add additional fields.
        cdef str field_name
        for field_name in self.data.minor_axis:
            if field_name in ['price', 'volume', 'dt', 'sid']:
                continue
            mapping[field_name] = (lambda x: x, field_name)

        return mapping

    @property
    def instance_hash(self):
        return self.arg_string
        
    def raw_data_gen(self):
        cdef unsigned int dt_idx, sid_idx, field_idx
        cdef str field, sid
        for dt_idx in range(len(self.arraydata)):
            df = self.arraydata[dt_idx]
            for sid_idx in range(len(df)):
                series = df[sid_idx]
                if np.isnan(series).any():
                    continue
                dt = self.dates[dt_idx]
                sid = self.sids[sid_idx]
                event = {'dt': dt,
                        'sid': sid,}
                for field_idx in range(len(series)):
                    field = self.fields[field_idx]
                    event[field] = series[field_idx]
                yield event
                    


    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data
