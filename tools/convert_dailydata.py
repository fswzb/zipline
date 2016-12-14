# -*- coding: utf-8 -*-
"""
Created on Tue May 10 21:34:52 2016

@author: Toby
"""
import pandas as pd
import pytz
field = 'cap'
#input goes here
#output filename, it must be "close","high","open","low","volume","xxx"(customized)
ofile = r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\%s"%(field, field)
'''
Sometimes you may want to split up the data and thus
have multiple files for different periods of the same type of data
'''

filelist = [r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2004_2005.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2006_2007.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2008.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2009.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2010.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2011.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2012.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2013.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2014.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2015.csv"%field,
            r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\DailyData\%s\2016.csv"%field,
            
           ]


###############################process#################################################
df_list = []
for f in filelist:
    df = pd.read_csv(f)
    df = df.convert_objects(convert_numeric=True)
    df = df.drop([0,1], axis = 0)
    try:
        df[u'Unnamed: 0'] = df[u'Unnamed: 0'].apply(lambda x:pd.datetime.strptime(x,'%Y/%m/%d'))
    except:
        df[u'Unnamed: 0'] = df[u'Unnamed: 0'].apply(lambda x:pd.datetime.strptime(x,'%Y-%m-%d'))
    df_list.append(df)
DF = pd.concat(df_list, axis = 0)
DF.index = DF[u'Unnamed: 0']
DF = DF.drop([u'Unnamed: 0'], axis=1)
DF = DF.dropna(axis = 1, how = 'all')
DF.index = map(lambda x:pytz.utc.localize(x), DF.index)
DF.to_pickle(ofile)
