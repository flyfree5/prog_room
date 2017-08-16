# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 13:35:25 2017

@author: fly
"""

import pandas as pd
import random

path = 'D:/mobai/program_python/split_data/split_data/'

data_all = []
for i_csv in range(199):
    data_all.append(pd.read_csv(path+str(i_csv)+'results.csv'))
    print i_csv

data_all.append(pd.read_csv(path+str(200)+'results.csv'))
data_a = pd.concat(data_all)
data_a = data_a.drop_duplicates(subset = 'orderid')
print len(data_a)

testdata = pd.read_csv('D:/mobai/data/test.csv')
print len(testdata)

cha_a_all = list(set(testdata.orderid)-set(data_a.orderid))
nul_v = random.sample(data_a.ix[:,['end_loc1','end_loc2','end_loc3']].values.tolist(),
                      len(cha_a_all))
data_cha = pd.DataFrame({'orderid':cha_a_all,
                         'end_loc1':[x[0] for x in nul_v],
                         'end_loc2':[x[1] for x in nul_v],
                         'end_loc3':[x[2] for x in nul_v]})
data_a = data_a.append(data_cha)
data_a.to_csv('results.csv',index=False,header = False)
    
#fl = open('test.txt','w+')
#for n in A[0:2]:
#    fl.write((n+'\n').encode("gb2312"))
#fl.close()















#