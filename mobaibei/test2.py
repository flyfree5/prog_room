# -*- coding: utf-8 -*-
"""
Created on Wed Aug 09 15:09:20 2017

@author: fly
"""

import random
import pandas as pd

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

start2end = pd.read_csv('testresults.csv')
testdata = read_testdata('D:/mobai/data/test.csv')
testd_re = pd.merge(testdata,start2end,how = 'left',on = ['geohashed_start_loc','hoursection'])
print sum(testd_re['end_loc1'].isnull())
re_f = testd_re.ix[0,['orderid','end_loc1','end_loc2','end_loc3']]
testd_re_gr_s = testd_re.groupby('geohashed_start_loc')
nnn=0
for n,m in testd_re_gr_s:
    logi_end1 = pd.isnull(m.end_loc1)
    logi_n_end1 = pd.notnull(m.end_loc1)
    s_le = sum(logi_end1)
    nnn+=1
    if s_le>0 and s_le<len(m):
        m_samp = m.ix[logi_n_end1,['end_loc1','end_loc2','end_loc3']]
        nul_v = random.sample(m_samp.values.tolist(),1)
        testd_re_gr_s.get_group(n).ix[logi_end1,'end_loc1'] = nul_v[0][0]
        testd_re_gr_s.get_group(n).ix[logi_end1,'end_loc2'] = nul_v[0][1]
        testd_re_gr_s.get_group(n).ix[logi_end1,'end_loc3'] = nul_v[0][2]
        print nnn,nul_v
    re_f = re_f.append(m.ix[:,['orderid','end_loc1','end_loc2','end_loc3']])
#    elif s_le==len(m)
#    for logi_i in logi_end1:
#        if
print sum(re_f['end_loc1'].isnull())
re_f = re_f.drop(0)
#print sum(testd_re['end_loc1'].isnull())
#re_f = testd_re.ix[:,['orderid','end_loc1','end_loc2','end_loc3']]
re_f.to_csv('results1.csv',index=False,header = False)