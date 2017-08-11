# -*- coding: utf-8 -*-
"""
Created on Wed Aug 09 15:09:20 2017

@author: fly
"""

import random
import pandas as pd
import datetime
import numpy as np

starttime = datetime.datetime.now()

def read_testdata(test_file):
#    test_file = 'D:/mobai/data/test.csv'
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
    return test_data

def dealwithdata(testd_re,num):
    testd_re = testd_re.reset_index(drop=True)
    re_f = testd_re.ix[0:1,['orderid','end_loc1','end_loc2','end_loc3']]
    testd_re = testd_re.groupby('geohashed_start_loc')
    nnn=0
    for n,m in testd_re:
        logi_end1 = pd.isnull(m['end_loc1'])
        logi_n_end1 = pd.notnull(m['end_loc1'])
        s_le = sum(logi_end1)
        nnn+=1
        if s_le>0 and s_le<len(m):
            m_samp = m.ix[logi_n_end1,['end_loc1','end_loc2','end_loc3']]
            nul_v = random.sample(m_samp.values.tolist(),1)
            m.ix[logi_end1,'end_loc1'] = nul_v[0][0]
            m.ix[logi_end1,'end_loc2'] = nul_v[0][1]
            m.ix[logi_end1,'end_loc3'] = nul_v[0][2]
            print nnn,nul_v
#            print sum(m['end_loc1'].isnull())
        elif s_le==len(m):
            nul_v = random.sample(start2end.ix[:,['end_loc1','end_loc2','end_loc3']].values.tolist(),1)
            m.ix[logi_end1,'end_loc1'] = nul_v[0][0]
            m.ix[logi_end1,'end_loc2'] = nul_v[0][1]
            m.ix[logi_end1,'end_loc3'] = nul_v[0][2]
            print nnn,nul_v
#            print sum(m['end_loc1'].isnull())
        re_f = re_f.append(m.ix[:,['orderid','end_loc1','end_loc2','end_loc3']])
        if str(nnn/200.0).isdigit():
            print sum(re_f['end_loc1'].isnull())

    print sum(re_f['end_loc1'].isnull())
    re_f = re_f.drop(0)
    re_f = re_f.drop(0)
    m_samplog = re_f['end_loc1'].isnull()
    nul_v = random.sample(re_f.ix[:,['end_loc1','end_loc2','end_loc3']].values.tolist(),1)
    re_f.ix[m_samplog,'end_loc1'] = nul_v[0][0]
    re_f.ix[m_samplog,'end_loc2'] = nul_v[0][1]
    re_f.ix[m_samplog,'end_loc3'] = nul_v[0][2]
    re_f.to_csv('split_data/'+str(num)+'results.csv',index=False)#,header = False)

num_split = 10000
start2end = pd.read_csv('testresults.csv')
testdata = read_testdata('D:/mobai/data/test.csv')
testd_reall = pd.merge(testdata,start2end,how = 'left',on = ['geohashed_start_loc','hoursection'])
print sum(testd_reall['end_loc1'].isnull())
n_s = len(testd_reall)/num_split
n_sorig = np.array(range(n_s))*num_split
n_send = np.array(range(n_s))*num_split+(num_split-1)
for num_ in range(n_s):
    s_time = datetime.datetime.now()
    t_d_r = testd_reall.ix[n_sorig[num_]:n_send[num_],:]
    dealwithdata(t_d_r,num_)
    e_time = datetime.datetime.now()
    print 'run time:',e_time-s_time
t_d_r = testd_reall.ix[(n_send[-1]+1):(len(testd_reall)-1),:]
dealwithdata(t_d_r,n_s)
endtime = datetime.datetime.now()
print 'run time:',endtime-starttime