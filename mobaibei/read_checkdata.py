# -*- coding: utf-8 -*-
"""
Created on Fri Aug 04 14:32:07 2017

@author: fly
"""

import pandas as pd

train_file = 'D:/mobai/data/train.csv'
train_data = pd.read_csv(train_file)
train_data['starttime'] = pd.to_datetime(train_data['starttime'], format='%Y-%m-%d %H:%M:%S')
hour_ = [x.hour for x in train_data.starttime]
train_data['hour'] = hour_
train_data['hoursection']=0
train_data.ix[(train_data.hour>=0)&(train_data.hour<5),'hoursection'] = 1
train_data.ix[(train_data.hour>=5)&(train_data.hour<10),'hoursection'] = 2
train_data.ix[(train_data.hour>=10)&(train_data.hour<16),'hoursection'] = 3
train_data.ix[(train_data.hour>=16)&(train_data.hour<21),'hoursection'] = 4
train_data.ix[(train_data.hour>=21),'hoursection'] = 5

test_file = 'D:/mobai/data/test.csv'
test_data = pd.read_csv(test_file)
test_data['starttime'] = pd.to_datetime(test_data['starttime'], format='%Y-%m-%d %H:%M:%S')
hour_ = [x.hour for x in test_data.starttime]
test_data['hour'] = hour_
test_data['hoursection']=0
test_data.ix[(test_data.hour>=0)&(test_data.hour<5),'hoursection'] = 1
test_data.ix[(test_data.hour>=5)&(test_data.hour<10),'hoursection'] = 2
test_data.ix[(test_data.hour>=10)&(test_data.hour<16),'hoursection'] = 3
test_data.ix[(test_data.hour>=16)&(test_data.hour<21),'hoursection'] = 4
test_data.ix[(test_data.hour>=21),'hoursection'] = 5
print test_data.head()
print train_data.head()
#====================
trainlocstart_grop = train_data.groupby('geohashed_start_loc')
testlocstart_grop = test_data.groupby('geohashed_start_loc')
for n_tlg,data_tg in trainlocstart_grop:
    print n_tlg
    trainend_grop_ins = data_tg.groupby('geohashed_end_loc')
    s_tgins = trainend_grop_ins.size().sort
#    s_tgins_df = pd.DataFrame({'index':s_tgins.index,'size':s_tgins})
    s_tgins.to_csv('D:/mobai/data/trainstartloc/'+n_tlg+'.csv')
#    for n,data_ in trainend_grop_ins:
        
#    s_tginsd = dict(list(s_tgins))
    


#==================
#==============================================================================
# train_data['day'] = [x.day for x in train_data.starttime]
# day_train_cha = train_data.starttime.max().day-train_data.starttime.min().day
# train_grop = train_data.groupby('userid')
# s_tg = train_grop.size()
# stg_ind = s_tg.index.tolist()
# 
# test_grop = test_data.groupby('userid')
# te_tg = test_grop.size()
# tetg_ind = te_tg.index.tolist()
# 
# test_cha_train = list(set(tetg_ind).difference(set(stg_ind))) # b中有而a中没有的
# #===================
# trainbike_grop = train_data.groupby('bikeid')
# testbike_grop = test_data.groupby('bikeid')
# sbike_tg = trainbike_grop.size()
# tebike_tg = testbike_grop.size()
# 
# #===============================
# traintime_grop = train_data.groupby('starttime')
# testtime_grop = test_data.groupby('starttime')
# stime_tg = traintime_grop.size()
# tetime_tg = testtime_grop.size()
#==============================================================================
