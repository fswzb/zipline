# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 21:24:09 2016

@author: Toby
"""
pos_24=results['positions'].ix[datetime.datetime(2015,9,24,15,0,0)]
pos_25=results['positions'].ix[datetime.datetime(2015,9,25,15,0,0)]
txn_25=results['transactions'].ix[datetime.datetime(2015,9,25,15,0,0)]
results['starting_cash'][datetime.datetime(2016,03,29,15,0,0)]
def cashflow(a):
    s=0
    for item in a:
        s=s+item['price']*item['amount']+item['commission']
    return s
    
def total(a):
    s=0
    for item in a:
        s=s+item['amount']*item['last_sale_price']
    return s
 
out_cash = 0
in_cash = 0   
for txn in txn_25:
    txn_sid = txn['sid']
    if txn['amount']<0:
        print "sell %s of %s at %s"%(abs(txn['amount']),txn_sid,txn['price'])
        in_cash+=txn['amount']*txn['price']
    if txn['amount']>0:
        print "buy %s of %s at %s"%(abs(txn['amount']),txn_sid,txn['price'])
        out_cash+=txn['amount']*txn['price']
    for pos in pos_24:
        if pos['sid'] == txn_sid:
            print "the old position price and amount are: ", pos['last_sale_price'], pos['amount']
    for pos2 in pos_25:
        if pos2['sid'] == txn_sid:
            print "the new position price and amount are: ", pos2['last_sale_price'], pos2['amount']
print "out_cash: %s"%out_cash
print "in_cash: %s"%in_cash    
net_cash_flow = out_cash + in_cash
print "net_cash_flow: %s"%net_cash_flow      
            
s_old=0 
s_new=0         
for pos in pos_24:
    pos_sid = pos['sid']
    for pos2 in pos_25:
        if pos_sid == pos2['sid']:
            print "old %s, %s, %s"%(pos_sid,pos['amount'],pos['last_sale_price'])
            print "new %s, %s, %s"%(pos_sid,pos2['amount'],pos2['last_sale_price'])
            s_old=s_old+pos['amount']*pos['last_sale_price']
            s_new=s_new+pos2['amount']*pos2['last_sale_price']
print "old value: %s, new value: %s"%(s_old, s_new)

s=0            
for pos in pos_25:
    pos_sid = pos['sid']
    flag = 0
    for pos2 in pos_24:
        if pos2['sid']==pos_sid:
            flag = 1
            break
    if flag == 0:
        print "new pos %s, %s, %s"%(pos_sid, pos['amount'], pos['last_sale_price'])
        s=s+pos['amount']*pos['last_sale_price']
print s

s=0        
for pos in pos_24:
    pos_sid = pos['sid']
    flag = 0
    for pos2 in pos_25:
        if pos2['sid']==pos_sid:
            flag = 1
            break
    if flag == 0:
        print "sold out pos %s, %s, %s"%(pos_sid, pos['amount'], pos['last_sale_price'])       
        s=s+pos['amount']*pos['last_sale_price']
print s