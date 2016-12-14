# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 22:14:33 2016

@author: Toby
"""

import sys
import MySQLdb
import traceback
import logging
import pandas as pd
from datetime import datetime
import pytz
import numpy as np

CONFIG = {'user':'yunneng', 'passwd':'yunneng', 'host':'59.67.147.50', 'db':'finchina2'}
DEFAULT_FIELDS = 'TCLOSE, THIGH, TLOW, TOPEN, PCHG, VOL, SECODE, TRADEDATE'
def getLogger():
    logger = logging.getLogger("DBProxy")
    if len(logger.handlers) == 0:
        hdlr = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    return logger
    
def report(function):
    def wrap(*args, **kwargs):
        wrap.call_count += 1
        indent = ' '*report._indent
        fc = "{}({})".format(function.__name__, ",".join(map(str, args) 
             + map(lambda (k,v): "{}={}".format(k,v), kwargs.items())))
        print("{}{} called #{}".format(indent, fc, wrap.call_count))
        report._indent += 1
        return_value = function(*args, **kwargs)
        report._indent += 1
        print("{}{} returned with value {}".format(indent, fc, str(return_value)))
        return return_value
    wrap.call_count = 0
    return wrap
report._indent = 0

class DBProxy:
    def __init__(self, config = CONFIG):
        self.config = config
        self.cnx = None
        self.logger = getLogger()
        
    def __del__(self):
        self.close()
    
    def getDbConn(self):
        if self.cnx is None:
            self.cnx = self.getDbConnection()
        return self.cnx
        
    def getCursor(self):
        try:
            if self.cnx is None or not self.cnx.is_connected():
                self.cnx = self.getDbConnection()
            return self.cnx.cursor()
        except:
            print "re-establish the connection"
            self.conx = self.getDbConnection()
            return self.cnx.cursor()
            
    def getDbConnection(self):
        cnx = None
        try:
            cnx = MySQLdb.connect(**self.config)
        except MySQLdb.Error as err:
            try:
                print "MySQL Error [%d]: %s" % (err.args[0], err.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(err)
            traceback.print_exception(*sys.exc_info())
        return cnx
        
    def execute(self, sqls, params = None):
        cursor = self.getCursor()
        for s in sqls:
            self.logger.debug(s)
            cursor.execute(s, params)
        self.commit()
        self.logger.debug("Total Statement Executed: " + str(len(sqls)))
        cursor.close()
            
    def callproc(self, func, params = None):
        cursor = self.getCursor()
        cursor.callproc(func, params)
        rs = []
        for result in cursor.stored_results():
            rs.append(result.fetchall())
        return rs
        
    def commit(self):
        if self.cnx is not None:
            self.cnx.commit()
        else:
            print("unable to find connection")
            
    def close(self):
        try:
            if self.cnx is not None:
                self.cnx.close()
            self.cnx = None
        except:
            traceback.print_exception(*sys.exc_info())
            
    def doQuery(self, sql, params = None):
        try:
            cursor = self.getCursor()
            cursor.execute(sql, params)
            data = cursor.fetchall()
            cursor.close()
            return data
        except MySQLdb.Error as err:
            print('Error SQL: ' + sql)
            traceback.print_exception(*sys.exc_info())
            try:
                print "MySQL Error [%d]: %s" % (err.args[0], err.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(err)
    
    def _get_sn_ts(self, startdate, enddate):
        sql = "select {} from finchina2.TQ_QT_SKDAILYPRICE where TCLOSE<>0 and TRADEDATE>=DATE('{}') and TRADEDATE<=DATE('{}') \
        and EXCHANGE in ('001002', '001003')".format(DEFAULT_FIELDS, startdate, enddate)
        #sql2= "select {} from finchina2.TQ_QT_INDEX where TRADEDATE>=DATE('{}') and TRADEDATE<=DATE('{}')".format(DEFAULT_FIELDS, startdate, enddate)
        print sql
        #print sql2
        res = self.doQuery(sql)
        res = np.array(res)
        #res2 = self.doQuery(sql2)
        #res2 = np.array(res2)
        res = pd.DataFrame(res, columns = ['price', 'high', 'low', 'open', 'ret', 'volume', 'sid', 'dt'])
        #res2 = pd.DataFrame(res2, columns = ['price', 'high', 'low', 'open', 'ret', 'volume', 'sid', 'dt'])
        #res = res.append(res2)
        res.fillna(np.nan, inplace = True)
        res.replace(0, np.nan, inplace = True)
        res.dropna(axis=0, how='any',subset=['dt','sid'], inplace=True)
        res.index = range(len(res))
        res.ix[:,'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        
        res.ix[:,'price'] = res['price'].apply(float)    

        res.ix[:,'high'] = res['high'].apply(float)
        
        res.ix[:,'low'] = res['low'].apply(float)
        
        res.ix[:,'open'] = res['open'].apply(float) 

        res.ix[:,'volume'] = res['volume'].apply(float) 
        
        res.ix[:,'ret'] = res['ret'].apply(float)
        
        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)
        print "stock series successfully queried."
        return res

    def _get_index_futures(self, startdate, enddate):
        sql = "select {} from finchina2.TQ_QT_SIFDAILYPRICE where TCLOSE<>0 and TRADEDATE>=DATE('{}')\
         and TRADEDATE<=DATE('{}')".format(DEFAULT_FIELDS, startdate, enddate)
        #sql2= "select {} from finchina2.TQ_QT_INDEX where TRADEDATE>=DATE('{}') and TRADEDATE<=DATE('{}')".format(DEFAULT_FIELDS, startdate, enddate)
        print sql
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['price', 'high', 'low', 'open', 'ret', 'volume', 'sid', 'dt'])
        res.fillna(np.nan, inplace = True)
        res.replace(0, np.nan, inplace = True)
        res.dropna(axis=0, how='any',subset=['dt','sid'], inplace=True)
        res.index = range(len(res))
        res.ix[:,'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        
        res.ix[:,'price'] = res['price'].apply(float)    

        res.ix[:,'high'] = res['high'].apply(float)
        
        res.ix[:,'low'] = res['low'].apply(float)
        
        res.ix[:,'open'] = res['open'].apply(float) 

        res.ix[:,'volume'] = res['volume'].apply(float) 
        
        res.ix[:,'ret'] = res['ret'].apply(float)
        
        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)
        print "index series successfully queried."
        return res
        
    def _get_index_ts(self, startdate, enddate):
        sql = "select {} from finchina2.TQ_QT_INDEX where TCLOSE<>0 and TRADEDATE>=DATE('{}')\
         and TRADEDATE<=DATE('{}')".format(DEFAULT_FIELDS, startdate, enddate)
        #sql2= "select {} from finchina2.TQ_QT_INDEX where TRADEDATE>=DATE('{}') and TRADEDATE<=DATE('{}')".format(DEFAULT_FIELDS, startdate, enddate)
        sql2 = "select SECODE from yunneng.INDEX_UNIVERSE"
        print sql
        print sql2
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['price', 'high', 'low', 'open', 'ret', 'volume', 'sid', 'dt'])
        res2 = self.doQuery(sql2)
        res2 = np.array(res2)
        res2 = pd.DataFrame(res2, columns = ['sid'])
        mask = [i for i in range(len(res)) if res.ix[i,'sid'] in res2['sid'].values]
        res = res.ix[mask,:]
        res.fillna(np.nan, inplace = True)
        res.replace(0, np.nan, inplace = True)
        res.dropna(axis=0, how='any',subset=['dt','sid'], inplace=True)
        res.index = range(len(res))
        res.ix[:,'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        
        res.ix[:,'price'] = res['price'].apply(float)    

        res.ix[:,'high'] = res['high'].apply(float)
        
        res.ix[:,'low'] = res['low'].apply(float)
        
        res.ix[:,'open'] = res['open'].apply(float) 

        res.ix[:,'volume'] = res['volume'].apply(float) 
        
        res.ix[:,'ret'] = res['ret'].apply(float)
        
        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)
        print "index future series successfully queried."
        return res
        
    def _get_dividends(self, startdate, enddate):
        sql = "select A.PUBLISHDATE, A.PUBLISHDATE, A.SECODE, A.SECODE, A.SYMBOL, DIVITYPE, EQURECORDDATE, XDRDATE, \
        AFTTAXCASHDV, PROBONUSRT, TRANADDRT, BONUSRT, ISNEWEST from finchina2.TQ_SK_DIVIDENTS as A inner join \
        finchina2.TQ_SK_BASICINFO as B  where A.COMPCODE=B.COMPCODE AND B.EXCHANGE in ('001002', '001003') AND \
        A.EQURECORDDATE BETWEEN '{}' AND '{}' AND A.ISNEWEST = 1".format(startdate, enddate)
        print sql
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['dt', 'declared_date', 'sid', 'payment_sid', 'symbol','type','ex_date',\
        'pay_date','net_amount','probonusrt','tranaddrt', 'bonusrt', 'isnewest'])
        res.replace(np.nan, 0, inplace = True)
        res.ix[:, 'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        res.ix[:, 'declared_date'] = res['declared_date'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        res.ix[:, 'ex_date'] = res['ex_date'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        res.ix[:, 'pay_date'] = res['pay_date'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        '''
        TODO:probonusrt and tranaddst equivalent?
        exdate not immediately followed by paydate?
        '''        

        res.ix[:,'net_amount'] = res['net_amount'].apply(lambda x:float(x)/10.0)
        res.ix[:,'probonusrt'] = res['probonusrt'].apply(lambda x:float(x)/10.0)
        res.ix[:,'tranaddrt'] = res['tranaddrt'].apply(lambda x:float(x)/10.0)
        res.ix[:,'bonusrt'] = res['bonusrt'].apply(lambda x:float(x)/10.0)

        res['ratio'] = res['probonusrt'] + res['tranaddrt'] + res['bonusrt']
        
        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)  
        print "dividend data successfully queried."
        return res
    

    def _get_fundamentals(self, field_dict, table):
        field, T = field_dict.items()[0]
        sql = "select A.PUBLISHDATE, A.ENDDATE, B.SECODE, A.{} from finchina2.{} as A inner join finchina2.TQ_SK_BASICINFO as B \
        on A.COMPCODE = B.COMPCODE where A.REPORTTYPE = 1 and B.EXCHANGE in ('001002', '001003')".format(field, table)  
        print sql
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['dt', 'rprtdate', 'sid', field])
        
        mask = [i for i, x in enumerate(res[field]) if x is not None]
        res.ix[mask, field] = res[field][mask].apply(T)

        mask = [i for i, x in enumerate(res['dt']) if x is not None]
        res.ix[mask,'dt'] = res['dt'][mask].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        
        mask = [i for i, x in enumerate(res['rprtdate']) if x is not None]
        #res.ix[mask,'rprtdate'] = res['rprtdate'][mask].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        res.sort(columns = ['dt'], inplace = True)
        res.index = res.sid.copy()
        res.drop_duplicates(subset=['sid','rprtdate'], inplace = True)
        print "fundamental data successfully queried."
        return res
        
    def _get_delist(self, startdate, enddate):
        sql = "select DELISTDATE, SECODE from finchina2.TQ_SK_BASICINFO where EXCHANGE in ('001002', '001003')\
        and DELISTDATE > DATE('19900101') and DELISTDATE >= DATE('{}') and DELISTDATE <= DATE('{}')".format(startdate, enddate)
        print sql
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['dt', 'sid'])
        res.ix[:, 'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        res['delist'] = 1
        
        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)
        res.drop_duplicates(inplace = True)
        print "delist events successfully queried."
        return res

    def _get_sn_ts_optional(self, field_dict, table, startdate, enddate):
        field_list = field_dict.keys()
        type_list = field_dict.values()
        fields = ','.join(field_list)
        sql = "select TRADEDATE, B.SECODE, A.{} from finchina2.{} as A inner join finchina2.TQ_SK_BASICINFO as B on A.SECODE = B.SECODE \
        where A.TRADEDATE>=DATE('{}') and A.TRADEDATE<=DATE('{}') and B.EXCHANGE in ('001002', '001003') ".format(fields, table, startdate, enddate) 
        print sql
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['dt', 'sid'] + field_list)
        res.ix[:, 'dt'] = res['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        for i, field in enumerate(field_list):
            mask = [j for j, x in enumerate(res[field]) if x is not None]
            res.ix[mask, field] = res[field][mask].apply(type_list[i])

        arrays = [res['dt'].values, res['sid'].values]
        tuples = list(zip(*arrays))
        mindex = pd.MultiIndex.from_tuples(tuples, names=['dt', 'sid'])
        res.index = mindex
        res = res.sortlevel(level = 0, axis = 0)
        res.drop_duplicates(inplace = True)
        print "optional data successfully queried."
        return res
        
    def _get_market_data(self, benchmk_secode, startdate = "20020101", enddate = datetime.today().strftime(r"%Y%m%d")):

        sql2 = "select TRADEDATE, TCLOSE from finchina2.TQ_QT_INDEX where SECODE = '{}' \
               and TRADEDATE >= DATE('{}') and TRADEDATE <= DATE('{}')".format(benchmk_secode, startdate, enddate)
        sql3 = "select TRADEDATE, M1, M3, M6, Y1, Y2, Y3, Y5, Y7, Y10, Y20, Y30 from finchina2.TREASURIES where \
               TRADEDATE >= DATE('{}') and TRADEDATE <= DATE('{}')".format(startdate, enddate)
        print sql2
        print sql3
        raw_bmk = self.doQuery(sql2)
        raw_bmk = np.array(raw_bmk)
        raw_tr_curves = self.doQuery(sql3)
        raw_tr_curves = np.array(raw_tr_curves)   
        raw_tr_curves = pd.DataFrame(raw_tr_curves, dtype = float, columns = ['Date', '1month', '3month', '6month', '1year', '2year',\
                                                            '3year', '5year', '7year', '10year', '20year', '30year'])
        raw_tr_curves['Date'] = raw_tr_curves['Date'].apply(lambda x: pytz.utc.localize(datetime.strptime(str(int(x)),r'%Y%m%d')))
        raw_tr_curves = raw_tr_curves.sort_index(axis = 0)
        raw_tr_curves['tid'] = range(1, len(raw_tr_curves) + 1)
        raw_tr_curves.index = raw_tr_curves['Date']
        tr_curves =raw_tr_curves
        
        bm = pd.DataFrame(raw_bmk, columns = ['dt', 'TCLOSE'])
        bm.index = bm['dt'].apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        bm = bm.sort_index(axis = 0)
        bm['RET'] = bm.TCLOSE.apply(float).pct_change()
        bm.ix[0,'RET'] = 0 #assign the first day return to 0 
        bm_returns = bm.drop(['dt', 'TCLOSE'], axis = 1)
        print "market data successfully queried."
        return (bm_returns, tr_curves)
                
    def _get_trading_dates(self, startdate, enddate):
        sql1 = "select TRADEDATE from finchina2.TQ_OA_TRDSCHEDULE where EXCHANGE = '001002' or EXCHANGE = '001003' \
               and TRADEDATE >= DATE('{}') and TRADEDATE <= DATE('{}')".format(startdate, enddate)  
        print sql1
        raw_tradingdates = self.doQuery(sql1)
        raw_tradingdates = np.array(raw_tradingdates)
        tradingdates = pd.DataFrame(raw_tradingdates, columns = ['Date'])
        tradingdates = tradingdates.Date.apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        tradingdates = pd.DatetimeIndex(tradingdates)
        print "trading dates successfully queried."
        return tradingdates

    def _get_trading_dates_tmp(self, startdate, enddate):
        sql1 = "select TRADEDATE from finchina2.TRDDATES \
               where TRADEDATE >= DATE('{}') and TRADEDATE <= DATE('{}')".format(startdate, enddate) 
        print sql1
        raw_tradingdates = self.doQuery(sql1)
        raw_tradingdates = np.array(raw_tradingdates)
        tradingdates = pd.DataFrame(raw_tradingdates, columns = ['Date'])
        tradingdates = tradingdates.Date.apply(lambda x: pytz.utc.localize(datetime.strptime(x,r'%Y%m%d')))
        tradingdates = pd.DatetimeIndex(tradingdates)
        print "trading dates successfully queried."
        return tradingdates
    
    def _get_secode_mapping(self):
        sql = "select SECODE, SYMBOL from finchina2.TQ_SK_BASICINFO"
        sql2 = "select SECODE, SYMBOL from finchina2.TQ_IX_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['sid', 'symbol'])
        res2 = self.doQuery(sql2)
        res2 = np.array(res2)
        res2 = pd.DataFrame(res2, columns = ['sid', 'symbol'])
        res = res.append(res2)
        res.index = res['sid']
        return res
    
    def _get_sk_secode_mapping(self):
        sql = "select SECODE, SYMBOL from finchina2.TQ_SK_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['sid', 'symbol'])
        res.index = res['sid']
        return res
    
    def _get_ix_secode_mapping(self):
        sql = "select SECODE, SYMBOL from finchina2.TQ_IX_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['sid', 'symbol'])
        res.index = res['sid']
        return res
   
    def _get_symbol_mapping(self):
        sql = "select SYMBOL, SECODE from finchina2.TQ_SK_BASICINFO"
        sql2 = "select SYMBOL, SECODE from finchina2.TQ_IX_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['symbol', 'sid'])
        res2 = self.doQuery(sql2)
        res2 = np.array(res2)
        res2 = pd.DataFrame(res2, columns = ['symbol', 'sid'])
        res = res.append(res2)
        res.index = res['symbol']
        return res
    
    def _get_sk_symbol_mapping(self):
        sql = "select SYMBOL, SECODE from finchina2.TQ_SK_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['symbol', 'sid'])
        res.index = res['symbol']
        return res
    
    def _get_ix_symbol_mapping(self):
        sql = "select SYMBOL, SECODE from finchina2.TQ_IX_BASICINFO"
        res = self.doQuery(sql)
        res = np.array(res)
        res = pd.DataFrame(res, columns = ['symbol', 'sid'])
        res.index = res['symbol']
        return res

    def _import_csv(self, csv):
         csv_data = pd.read_csv(csv)
         cursor = self.getCursor()
         for i, row in csv_data.iterrows():
             print row[0]
             row[0] = (datetime.strptime(row[0], r'%Y/%m/%d')).strftime(r"%Y%m%d")  
             sql = "INSERT INTO finchina2.TREASURIES(TRADEDATE) VALUES('%s')"%row[0]
             print sql
             cursor.execute(sql)
         self.commit()
         cursor.close()
         
    def _import_csv1(self, csv):
         csv_data = pd.read_csv(csv)
         cursor = self.getCursor()
         for i, row in csv_data.iterrows():
             print row[0]
             row[0] = (datetime.strptime(row[0], r'%Y/%m/%d')).strftime(r"%Y%m%d")       
             cursor.execute("INSERT INTO finchina2.TREASURIES(TRADEDATE, M1, M3, M6, Y1, Y2, Y3, Y5,\
                            Y7, Y10, Y20, Y30) VALUES('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"%tuple(row.values))
         self.commit()
         cursor.close()

    def _import_benchmarks(self, csv):
         csv_data = pd.read_csv(csv)
         cursor = self.getCursor()
         for i, row in csv_data.iterrows():
             print row[0]
             row[0] = (datetime.strptime(row[0], r'%Y/%m/%d')).strftime(r"%Y%m%d")  
             sql1 = "INSERT INTO yunneng.TQ_QT_INDEX(TRADEDATE, SECODE, TCLOSE) VALUES('%s', '%s', %s)"%(row[0], '000001.SH', row[1])
             sql2 = "INSERT INTO yunneng.TQ_QT_INDEX(TRADEDATE, SECODE, TCLOSE) VALUES('%s', '%s', %s)"%(row[0], '000300.SH', row[2])
             cursor.execute(sql1)
             cursor.execute(sql2)
         self.commit()
         cursor.close()
         
    def _import_mapping(self,csv):
         csv_data = pd.read_csv(csv)
         cursor = self.getCursor()
         for i, row in csv_data.iterrows():
             print row[0]  
             sql1 = "INSERT INTO yunneng.SECODES(SECODE, SYMBOL) VALUES('%s', '%s')"%(row[0], row[1])
             cursor.execute(sql1)
         self.commit()
         cursor.close()
         
    def _import_index_universe(self,csv):
         csv_data = pd.read_csv(csv)
         cursor = self.getCursor()
         for i, row in csv_data.iterrows():
             print row[0]  
             sql1 = "INSERT INTO yunneng.INDEX_UNIVERSE(SECODE) VALUES('%s')"%(row[0])
             cursor.execute(sql1)
         self.commit()
         cursor.close()
         
if __name__ == "__main__":
    dbProxy = DBProxy(CONFIG)
    #field_list = ['TOTMKTCAP']
    #res = dbProxy._get_sn_ts(field_list,'20140101','20140131')
    #dbProxy._import_csv(r"E:\Anaconda\Lib\site-packages\zipline_china\zipline\cache\MarketConfig\treasuries.csv")
    startdate = '20141101'
    enddate = '20150331'
    benchmark = '2070000061'
    #csv = r"D:\Data\MarketConfig\benchmark_daily2.CSV"
    #tradingdates = dbProxy._get_trading_dates('20020101', datetime.today().strftime(r"%Y%m%d"))
    #dbProxy._import_csv(r'D:\Data\MarketConfig\dates.csv')
    #res = dbProxy._get_market_data('000001.SH', startdate = "20020101", enddate = datetime.today().strftime(r"%Y%m%d"))
    #res = dbProxy._get_sn_ts(startdate,enddate)
    #res = dbProxy._get_sn_ts(startdate, enddate)
    #res = dbProxy._get_dividends(startdate, enddate)
    #tradingdates = dbProxy._get_trading_dates(startdate, enddate)
    #dbProxy._import_csv(r"D:\Data\MarketConfig\dates2.csv")
    #dbProxy._import_benchmarks(csv)
    #dbProxy._import_mapping(r'D:\Data\mapping\secode.csv')
    #accorece = dbProxy._get_fundamentals({'ACCORECE': float}, 'tq_fin_probalsheetnew')
    #datasource = dbProxy._get_sn_ts_optional({'TOTMKTCAP': float}, 'tq_qt_skdailyprice', startdate, enddate)
    #dbProxy._import_index_universe(r'E:\yunneng\index_universe.csv')
    index_universe = dbProxy._get_index_ts(startdate, enddate)
    
