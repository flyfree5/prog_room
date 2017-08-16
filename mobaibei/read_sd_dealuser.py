# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 11:19:21 2017

@author: fly
"""

import pandas as pd
import random

#def read_testdata(test_file):
##    test_file = 'D:/mobai/data/test.csv'
#    test_data = pd.read_csv(test_file)
#    test_data['starttime'] = pd.to_datetime(test_data['starttime'], format='%Y-%m-%d %H:%M:%S')
#    hour_ = [x.hour for x in test_data.starttime]
#    test_data['hour'] = hour_
#    test_data['hoursection']=0
#    test_data.ix[(test_data.hour>=0)&(test_data.hour<5),'hoursection'] = 1
#    test_data.ix[(test_data.hour>=5)&(test_data.hour<10),'hoursection'] = 2
#    test_data.ix[(test_data.hour>=10)&(test_data.hour<16),'hoursection'] = 3
#    test_data.ix[(test_data.hour>=16)&(test_data.hour<21),'hoursection'] = 4
#    test_data.ix[(test_data.hour>=21),'hoursection'] = 5
#    return test_data

path = 'split_data/split_data/'
user_pred = pd.read_csv('D:/mobai/program_python/user_pre.csv')
#test_data = read_testdata('D:/mobai/data/test.csv')
#test_data = test_data[['orderid','userid','hoursection']]
#test_data.to_csv('orderuserhour.csv',index=False)
test_data = pd.read_csv('orderuserhour.csv')
data_all = []
for i_csv in range(201):
    i_data = pd.read_csv(path+str(i_csv)+'results.csv')
    i_data = pd.merge(i_data,test_data,how='left',on='orderid')
    i_d = pd.merge(i_data,user_pred,how='left',on=['userid','hoursection'])
    print i_d.end_loc1.head()
    na_i_d = i_d.end_loc.isnull()
    if not sum(na_i_d)==0:
        for n_,na_i in enumerate(na_i_d):
            if not na_i:
                i_d.ix[n_,'end_loc1'] = i_d.end_loc[n_][2:9]
                i_d.ix[n_,'end_loc2'] = i_d.end_loc[n_][13:20]
                i_d.ix[n_,'end_loc3'] = i_d.end_loc[n_][24:31]
    data_all.append(i_d[['orderid','end_loc1','end_loc2','end_loc3']])
    print i_csv,sum(i_d.end_loc1.isnull())
    print i_d.end_loc1.head()
data_a = pd.concat(data_all)
cha_a_all = list(set(test_data.orderid)-set(data_a.orderid))
nul_v = random.sample(data_a.ix[:,['end_loc1','end_loc2','end_loc3']].values.tolist(),
                      len(cha_a_all))
data_cha = pd.DataFrame({'orderid':cha_a_all,
                         'end_loc1':[x[0] for x in nul_v],
                         'end_loc2':[x[1] for x in nul_v],
                         'end_loc3':[x[2] for x in nul_v]})
data_a = data_a.drop_duplicates(subset = 'orderid')
data_a = data_a.append(data_cha)
data_a = data_a.ix[:,['orderid','end_loc1','end_loc2','end_loc3']]
data_a.to_csv('results.csv',index=False,header = False)





#