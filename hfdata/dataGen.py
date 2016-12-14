# -*- coding: utf-8 -*-
"""
Created on Tue Apr 05 00:06:57 2016

@author: Toby
"""

import pandas as pd
import datetime
import pytz
import os

indirectory = "D:\Data\MinuteData\\2015\\2015.7-2015.12\\2015.7-2015.12"
outdirectory = "D:\Data\MinuteData\\2015\\2015.7-2015.12\\2015.7-2015.12_new"

def processfiles(indirectory, outdirectory):
    filelist = os.listdir(indirectory)
    total = len(filelist)
    i = 0.0
    for f in filelist:
        filename_in = indirectory + "\\" + f
        filename_out = outdirectory + "\\" + f
        df = pd.read_csv(filename_in, names = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
        df.to_csv(filename_out, index = False)
        i = i + 1
        print "progress: %d%%"%(i*100.0/total)
        
def generate_data(filename):
    sid = filename.split('\\')[-1].split('.')[0]
    sid = sid[2:]+'.'+sid[:2]
    sid.encode("utf-8")
    df = pd.read_csv(filename)
    date = df.date
    time = df.time
    dts = map(lambda x, y: x+' '+y, date, time)
    date = map(lambda x: datetime.datetime.strptime(x, "%Y/%m/%d %H:%M"), dts)
    date = map(lambda x:pytz.utc.localize(x), date)
    price = df.close.values
    volume = df.volume.values
    new_df = pd.DataFrame({sid : price, 'volume' : volume}, index = date)
    new_df.name = sid
    return new_df

def generate_source(outdirectory):
    filelist = os.listdir(outdirectory)
    total = len(filelist)
    i = 0.0
    source = []
    for f in filelist:
        filename = outdirectory + "\\" + f
        df = generate_data(filename)
        source.append(df)
        i = i + 1
        print "progress: %d%%"%(i * 100.0/total)        
    return source
        