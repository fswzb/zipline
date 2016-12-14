# -*- coding: utf-8 -*-
"""
Created on Tue May 10 23:43:12 2016

@author: Toby
"""
import sys
sys.path.insert(0,"E:\Anaconda\Lib\site-packages\zipline_china")
from zipline.hfdata.dataGen import processfiles
from zipline.hfdata.dataGen import generate_source
import pickle

#input goes here
indirectory = r"D:\Data\MinuteData\\2015\\2015.7-2015.12\\2015.7-2015.12"
outdirectory = r"D:\Data\MinuteData\\2015\\2015.7-2015.12\\2015.7-2015.12_new"
ofile = r"D:\Data\hfData"
#####################################process###################################
processfiles(indirectory, outdirectory)
data = generate_source(outdirectory)
with open(ofile,'w') as f:
    pickle.dump(data, f)