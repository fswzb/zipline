# -*- coding: utf-8 -*-
"""
Created on Sun Nov 13 21:50:18 2016

@author: Toby
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
ss=0.03*np.random.randn(40)
cum_ss = (1+ss/100.0).cumprod(axis=0)
def find_loc_peak(ss,days=5):
    ss = pd.Series(ss)
    loc_surge = set()
    for window in range(5):
        cum_ret = pd.rolling_apply(ss,window,lambda x:np.prod(1+x)-1)
        is_surge = (cum_ret > 0.05) & (cum_ret < 0.5) & (ss > 0.0)
        loc_surge = loc_surge.union(is_surge[is_surge == True].index)
    
    loc_peak = list()    
    for loc in loc_surge:
        i = 0
        while ss[loc+i] > 0.0:
            i = i+1
            if i >= (40-loc):
                break
        loc_peak.append(loc+i-1)
        
    loc_peak = set(loc_peak)
    return loc_peak
    
loc_peak = find_loc_peak(ss)
ax1 = plt.subplot(1, 1, 1)
ax1.plot(cum_ss)
ax1.scatter(list(loc_peak),cum_ss[list(loc_peak)])    

                
        
    