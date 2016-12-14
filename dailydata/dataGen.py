# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 17:08:30 2016

@author: Toby
"""

import pandas as pd
import pytz



class dailyData(object):
    def __init__(self, indirectory = None):
        self.allsids = None
        self.rootdirectory = indirectory
        self.regular_source = {}
        self.custom_source = {}
        self.merged_source = {}
        self.supported_sourcetype = frozenset({'csv', 'pickle'})

        
    def __call__(self, *args, **kwargs):
        return self.generate_datapanel(self, *args, **kwargs)
        
    def combine_source(self, *args, **kwargs):
        custom_source_type = kwargs.pop('CustomSourceType', None)
        filelist = kwargs.pop('CustomFileList', [])
        if custom_source_type not in self.supported_sourcetype:
            self.merged_source = self.regular_source
            return
        if not filelist:
            self.merged_source = self.regular_source
            return        
        for stock in self.allsids:
            self.merged_source[stock] = pd.concat([self.custom_source[stock], self.regular_source[stock]], axis = 1)
        

    def generate_regulardatapanel(self, *args, **kwargs):  
        regular_source_type = kwargs.pop('RegularSourceType', None)
        regular_source_directory = kwargs.pop('RegularSourceDirectory', None)
        if not regular_source_type or not regular_source_type:
            print "must provide regular source type and directory!"

        self.rootdirectory = regular_source_directory
        if regular_source_type == 'csv':
            dateparse = lambda x: pd.datetime.strptime(x, '%Y/%m/%d')
            filename_close = self.rootdirectory + '\\' + 'close.csv'
            close = pd.read_csv(filename_close, parse_dates = ['Date'], date_parser = dateparse, index_col = 'Date')
            if not close.index.tzinfo:
                close.index = map(lambda x:pytz.utc.localize(x), close.index)
            self.allsids = close.columns
            self.index = close.index
            filename_volume = self.rootdirectory + '\\' + 'volume.csv'
            volume = pd.read_csv(filename_volume, parse_dates = ['Date'], date_parser = dateparse, index_col = 'Date')
            if not volume.index.tzinfo:
                volume.index = map(lambda x:pytz.utc.localize(x), volume.index)
            filename_open = self.rootdirectory + '\\' + 'open.csv'
            Open = pd.read_csv(filename_open, parse_dates = ['Date'], date_parser = dateparse, index_col = 'Date')
            if not Open.index.tzinfo:
                Open.index = map(lambda x:pytz.utc.localize(x), Open.index)
            filename_high = self.rootdirectory + '\\' + 'high.csv'
            high = pd.read_csv(filename_high, parse_dates = ['Date'], date_parser = dateparse, index_col = 'Date') 
            if not high.index.tzinfo:
                high.index = map(lambda x:pytz.utc.localize(x), high.index)
            filename_low = self.rootdirectory + '\\' + 'low.csv'
            low = pd.read_csv(filename_low, parse_dates = ['Date'], date_parser = dateparse, index_col = 'Date') 
            if not low.index.tzinfo:
                low.index = map(lambda x:pytz.utc.localize(x), low.index)
        elif regular_source_type == 'pickle':
            filename_close = self.rootdirectory + '\\' + 'close'
            close = pd.read_pickle(filename_close)
            if not close.index.tzinfo:
                close.index = map(lambda x:pytz.utc.localize(x), close.index)
            self.allsids = close.columns
            filename_volume = self.rootdirectory + '\\' + 'volume'
            volume = pd.read_pickle(filename_volume)
            if not volume.index.tzinfo:
                volume.index = map(lambda x:pytz.utc.localize(x), volume.index) 
            filename_high = self.rootdirectory + '\\' + 'high'
            high = pd.read_pickle(filename_high)
            if not high.index.tzinfo:
                high.index = map(lambda x:pytz.utc.localize(x), high.index) 
            filename_low = self.rootdirectory + '\\' + 'low'
            low = pd.read_pickle(filename_low)
            if not low.index.tzinfo:
                low.index = map(lambda x:pytz.utc.localize(x), low.index) 
            filename_open = self.rootdirectory + '\\' + 'open'
            Open = pd.read_pickle(filename_open)
            if not Open.index.tzinfo:
                Open.index = map(lambda x:pytz.utc.localize(x), Open.index)
        for stock in close.columns:
            close_series = close[stock]
            volume_series = volume[stock]
            Open_series = Open[stock]
            low_series = low[stock]
            high_series = high[stock]
            df_for_sid = pd.DataFrame({'price': close_series.values, 'volume': volume_series.values, 'open': Open_series.values,\
                               'low': low_series.values, 'high': high_series.values},\
                            index = close_series.index)
            self.regular_source[stock] = df_for_sid

            
    def generate_customdatapanel(self, *args, **kwargs):  
        custom_source_type = kwargs.pop('CustomSourceType', None)
        filelist = kwargs.pop('CustomFileList', [])
        if custom_source_type not in self.supported_sourcetype:
            return
        if not filelist:
            return
            
        frames = {}   
        if custom_source_type == "pickle":
            for f in filelist:
                variable = f.split('\\')[-1]
                raw_data = pd.read_pickle(f)
                if raw_data.index.tzinfo == None:
                    raw_data.index =map(lambda x:pytz.utc.localize(x), raw_data.index)
                frames[variable] = raw_data
            allsids = [[sid for sid in df.columns] for df in frames.values()]
            allsids = allsids[0]
            index_for_sid_df = raw_data.index
            for stock in allsids:
                seriesContainer = {}
                for variable, df in frames.iteritems():
                    seriesContainer[variable] = df[stock]
                
                df_for_sid = pd.DataFrame(seriesContainer, index = index_for_sid_df)
                self.custom_source[stock] = df_for_sid
    def rawpanel_datatype_check(self, pn):
        pn.items = map(lambda x:x.encode("utf-8"),pn.items)
        pn.minor_axis = map(lambda x:x.encode("utf-8"),pn.minor_axis)
        return pn
    def generate_datapanel(self, *args, **kwargs):
        self.generate_regulardatapanel(self, *args, **kwargs)
        self.generate_customdatapanel(self, *args, **kwargs)
        self.combine_source(self, *args, **kwargs)
        pn = pd.Panel(self.merged_source)
        Panel = self.rawpanel_datatype_check(pn)
        return Panel