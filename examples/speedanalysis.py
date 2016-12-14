# -*- coding: utf-8 -*-
"""
Created on Fri May 06 20:16:56 2016

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
from operator import attrgetter
from itertools import groupby
from zipline.sources.data_frame_source import DataPanelSource
from zipline.gens.composites import (
    date_sorted_sources,
    sequential_transforms,
)

indirectory = r"D:\Data\DailyData"
source = dataGen.dailyData()
datasource = source(CustomSourceType = 'pickle', RegularSourceType = 'pickle', RegularSourceDirectory = r'D:\Data\DailyData',\
              CustomFileList = [r'D:\Data\DailyData\cap'])

data = DataPanelSource(datasource)

date_sorted = date_sorted_sources(data)

data_gen = groupby(date_sorted, attrgetter('dt'))
t = time.clock()
for date, snapshot in data_gen:
    print "date: %s"%date
    previous_time = t
    t = time.clock()
    dt = t - previous_time
    print dt
    for event in snapshot:
        continue