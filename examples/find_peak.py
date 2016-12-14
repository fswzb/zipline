# -*- coding: utf-8 -*-
"""
Created on Sun Nov 13 21:50:18 2016

@author: Toby
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
ss=0.03*np.random.randn(40)
cum_ss = (1+ss).cumprod(axis=0)
def find_surge(ss,days=5):
    ss = pd.Series(ss)
    loc_surge = set()
    surge_rate = pd.Series(index = range(len(ss)))
    anchor_loc = pd.Series(index = range(len(ss)))
    for window in range(5,0,-1):
        cum_ret = pd.rolling_apply(ss,window,lambda x:np.prod(1+x)-1)
        is_surge = (cum_ret > 0.05) & (cum_ret < 0.5) & (ss > 0.0)
        loc = is_surge[is_surge == True].index
        loc_surge = loc_surge.union(is_surge[is_surge == True].index)
        anchor_loc[loc] = np.array(loc)-(window-1)
        surge_rate[loc] = cum_ret[loc]
        
    
    loc_peak = pd.Series(index = range(len(ss)))
    for loc in loc_surge:
        i = 0
        while ss[loc+i] > 0.0:
            i = i+1
            if i >= (40-loc):
                break
        loc_peak[loc] = (loc+i-1)
        anchor_loc[loc+i-1] = anchor_loc[loc] #the anchor location of the peak point inherited from the previous surge point
    return loc_peak, anchor_loc


def find_peak(ss, days=5):
    surge_rate = pd.Series(index = range(len(ss)))
    loc_peak, anchor_loc = find_surge(ss,days)
    loc_peak = loc_peak[loc_peak.notnull()]
    loc_peak_list = list(set(loc_peak[loc_peak.notnull()].values))

    for loc in loc_peak_list:
        anchor = anchor_loc[loc]
        while anchor>0 and ss[anchor-1] > 0.0:
            anchor = anchor - 1
            if anchor<=0:
                break
        anchor_loc[loc] = anchor  #push the anchor point to the left until ret<0
        surge_rate[loc] = (np.prod(1+ss[anchor:loc+1])-1)
    surge_rates = zip(loc_peak_list,surge_rate[loc_peak_list])
    levels = zip(loc_peak_list,cum_ss[loc_peak_list])
    return loc_peak_list, surge_rates
    
s = np.random.randn(1000,40)
for i in range(len(s)):
    find_peak(s[i,:])
    
loc_peak_list, surge_rates = find_peak(ss)
fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)
plt.plot(cum_ss)
plt.scatter(loc_peak_list,cum_ss[loc_peak_list])  
for i,j in surge_rates:
    ax1.annotate('({:d},{:.2%})'.format(int(i),j), (i,cum_ss[i]), textcoords='data') 
plt.grid()
fig.show()

        
    