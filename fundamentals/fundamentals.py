# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 23:45:31 2016

@author: Toby
"""
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

class Fundamental(object):
    def __init__(self, field, sid_universe, data):
        self.field = field
        self.rawdata = data
        self.process_raw_fundamental()
        self.sid_universe = set(sid_universe)
        
    def _update_universe(self, sids):
        self.sid_universe = sorted(set(sids))
        
    def _get_fundamentals(self, dt, nlookback = 1):
        
        mask = self.announcement <= dt
        res = self.df[mask].copy()
        res = res.ix[self.sid_universe]
        res.dropna(axis = 1, how = 'all', inplace = True)

        return res  

    def process_raw_fundamental(self):
        rprtdates = np.sort(np.unique(self.rawdata['rprtdate'].values))[::-1]
        self.quarterdata_list = []
        self.announcement_list = []
        for date in rprtdates:
            s1 = self.rawdata[self.rawdata['rprtdate'] == date][self.field].copy()
            s1.name = date
            self.quarterdata_list.append(s1)
            s2 = self.rawdata[self.rawdata['rprtdate'] == date]['dt'].copy()
            s2.name = date
            self.announcement_list.append(s2)
        self.df = pd.concat(self.quarterdata_list, axis=1)
        self.announcement = pd.concat(self.announcement_list, axis=1)
        self.announcement.fillna(pytz.utc.localize(datetime(2020,1,1)), inplace = True)
            