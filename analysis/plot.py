# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 14:27:05 2016

@author: e777973
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class plot2(object):
    def __init__(self,
                 algorithm_returns = None,
                 benchmark_returns = None,
                 index = None):

        self.algorithm_returns = algorithm_returns
        self.benchmark_returns = benchmark_returns
        self.index = index
        self.cumulative_returns_tsplot = plt.figure(1)
        self.algorithm_returns_plot = plt.figure(2)        
        self.plot_objects = {'algorithm_returns': self.algorithm_returns_plot,\
                             'cumulative_returns': self.cumulative_returns_tsplot}

    def _show(self, name):
        if name == 'cumulative_returns':
            figure = self.plot_objects[name]
            Ax = figure.add_subplot(111)
            Ax.plot(self.algorithm_returns)
            figure.show()
        elif name == 'algorithm_returns:':
            figure = self.plot_objects[name]
            Ax = figure.add_subplot(211)
            Ax.plot(self.algorithm_returns)
            
            Ax = figure.add_subplot(212)
            Ax.hist(self.algorithm_returns)
            figure.show()                         

class plot(object):
    def __init__(self,
                 dates = None,
                 algorithm_returns = None,
                 benchmark_returns = None):
        self.algorithm_returns = algorithm_returns
        self.benchmark_returns = benchmark_returns
        self.cumulative_returns = (self.algorithm_returns+1).cumprod()
        self.drawdown = drawdown(self.cumulative_returns+1)
        self.dates = dates
    def _show(self):
        ax1 = plt.subplot(2, 2, 1)
        ax1.plot(self.dates, self.cumulative_returns, 'k')
        ax1.set_xlabel('date')
        ax1.set_ylabel('cumulative returns')
        
        ax2 = plt.subplot(2, 2, 2)
        ax2.bar(self.dates, self.algorithm_returns)
        ax2.set_xlabel('date')
        ax2.set_ylabel('returns')
        
        ax3 = plt.subplot(2,2,3)
        ax3.fill_between(self.dates, 0, self.drawdown)
        plt.show()
        
        ax4 = plt.subplot(2,2,4)
        ax4.hist(self.algorithm_returns)
        plt.show()
        

def drawdown(xs):
    dd = (np.maximum.accumulate(xs) - xs)/np.maximum.accumulate(xs) # drawdown for each point in time
    return dd
    
if __name__ == '__main__':
    p = plot(pd.date_range('20140101','20140120'),np.random.randn(20), np.random.randn(20))
    p._show()