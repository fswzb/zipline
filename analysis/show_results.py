# -*- coding: utf-8 -*-
'''
回测结果可视化
'''
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick 
#%matplotlib inline


def drawdown(xs):
    dd = (np.maximum.accumulate(xs) - xs)/np.maximum.accumulate(xs) # drawdown for each point in time
    return dd

def count(algo):
    cumulative_risk_metrics = algo.perf_tracker.cumulative_risk_metrics
    results_count = pd.DataFrame()
    results_count['algorithm_returns'] = cumulative_risk_metrics.algorithm_returns
    results_count['benchmark_returns'] = cumulative_risk_metrics.benchmark_returns
    results_count['algorithm_cumulative_returns'] = cumulative_risk_metrics.algorithm_cumulative_returns
    results_count['benchmark_cumulative_returns'] = cumulative_risk_metrics.benchmark_cumulative_returns
    results_count['drawdowns'] = cumulative_risk_metrics.drawdowns
    return results_count

def plot_results(algo):
    results_count = count(algo)
    dates = results_count.index
    algorithm_returns = results_count['algorithm_returns']
    algorithm_cumulative_returns = results_count['algorithm_cumulative_returns']
    benchmark_cumulative_returns = results_count['benchmark_cumulative_returns']
    drawdowns = results_count['drawdowns']

    fig = plt.figure(figsize=(16,10))
    yticks = mtick.FormatStrFormatter('%.0f%%')
        
    ax1 = plt.subplot(3, 1, 1)
    plt.plot(dates, algorithm_cumulative_returns*100, lw=1.5, label='algorithm')
    plt.plot(dates, benchmark_cumulative_returns*100,'r', lw=1.5, label='benchmark')
    ax1.yaxis.set_major_formatter(yticks) 
    plt.grid(True)
    plt.legend(loc=0)
    plt.xlabel('date')
    plt.ylabel('cumulative returns')        
        
    ax2 = plt.subplot(3, 1, 2)
    plt.bar(dates, algorithm_returns*100, lw=0.5)
    ax2.yaxis.set_major_formatter(yticks) 
    plt.grid(True)
    plt.xlabel('date')
    plt.ylabel('daily returns')
        
    ax3 = plt.subplot(3, 1, 3)
    plt.plot(dates, drawdowns*100, lw=1.5)
    ax3.yaxis.set_major_formatter(yticks)
    plt.grid(True)
    plt.xlabel('date')
    plt.ylabel('drawdown')
        
def results_metrics(algo):
    results_count = count(algo)
    dates = results_count.index
    cumulative_risk_metrics = algo.perf_tracker.cumulative_risk_metrics
    algo_metrics = algo.perf_tracker.cumulative_risk_metrics.metrics.iloc[-1]
    
    algo_return =  results_count['algorithm_cumulative_returns'][-1]
    bench_return = results_count['benchmark_cumulative_returns'][-1]
    year_return = (1+algo_return)**(250./len(dates))-1
    bench_year_return = (1+bench_return)**(250./len(dates))-1
    max_drawdown = cumulative_risk_metrics.max_drawdown
    
    alpha = algo_metrics['alpha']
    beta = algo_metrics['beta']
    sharpe = algo_metrics['sharpe']
    algorithm_volatility = algo_metrics['algorithm_volatility']
    benchmark_volatility = algo_metrics['benchmark_volatility']
    downside_risk = algo_metrics['downside_risk']
    sortino = algo_metrics['sortino']
    information = algo_metrics['information']
    
    results_metrics = pd.DataFrame()
    results_metrics[u'策略收益'] = pd.DataFrame(["%.2f%%" %(algo_return*100)])
    results_metrics[u'策略年化收益'] = "%.2f%%" %(year_return*100)
    results_metrics[u'基准收益'] = "%.2f%%" %(bench_return*100)
    results_metrics[u'基准年化收益'] = "%.2f%%" %(bench_year_return*100)
    results_metrics[u'最大回撤'] = "%.2f%%" %(max_drawdown*100)
    results_metrics['alpha'] = np.round(alpha,2)
    results_metrics['beta'] = np.round(beta,2)
    results_metrics['sharpe'] = np.round(sharpe,2)
    results_metrics['algorithm volatility'] = np.round(algorithm_volatility,2)
    results_metrics['benchmark volatility'] = np.round(benchmark_volatility,2)
    results_metrics['downside risk'] = np.round(downside_risk,2)
    results_metrics['sortino'] = np.round(sortino,2)
    results_metrics['information'] = np.round(information,2)
    
    return results_metrics
    #print u'策略收益    策略年化收益     基准收益    基准年化收益    最大回撤    \
    #      alpha    beta    sharpe    algorithm_volatility    '
    #print '%.2f%%        %.2f%%          %.2f%%        %.2f%%        %.2f%% '\
    #    %(algo_return*100, year_return*100, bench_return*100, bench_year_return*100, max_drawdown*100)

class visualize:
    def __init__(self, algo):
        self.algo = algo
        
    def __call__(self):   
        self.results_count = count(self.algo)
        self.results_plot = plot_results(self.algo)
        self.results_metrics = results_metrics(self.algo)
        return self.results_metrics
        
        