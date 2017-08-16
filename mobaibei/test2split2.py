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

def dealwithdata(testd_re,num,tr_data):
    testd_re = testd_re.reset_index(drop=True)
    re_f = testd_re.ix[0:1,['orderid','end_loc1','end_loc2','end_loc3']]
    re_f.ix[0,'end_loc1']=0
    re_f.ix[1,'end_loc1']=0
    testd_re = testd_re.groupby('geohashed_start_loc')
    nnn=0
    for n,m in testd_re:
#	print n,len(m)
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
#            print '1',nnn,nul_v,n[0:5]
#            print sum(m['end_loc1'].isnull())
        elif s_le==len(m):
            tr_ = tr_data.get_group(n[0:5])
#	    print tr_.geohashed_end_loc.head()
            trg_all = tr_.geohashed_end_loc.tolist()
#	    print len(trg_all),trg_all[0:6]
            #trg_all_ = ]
            trg_all.extend(tr_.geohashed_start_loc.tolist())
#	    print len(trg_all),trg_all[0:6]
	    if len(trg_all)>2:
                nul_v = random.sample(trg_all,3)
            elif len(trg_all)==2:
                nul_v = [trg_all[0],trg_all[0],trg_all[1]]
            elif len(trg_all)==1:
                nul_v = [trg_all[0]]*3
            m.ix[logi_end1,'end_loc1'] = nul_v[0]
            m.ix[logi_end1,'end_loc2'] = nul_v[1]
            m.ix[logi_end1,'end_loc3'] = nul_v[2]
#            print '2',nnn,nul_v,n[0:5]
#            print sum(m['end_loc1'].isnull())
        re_f = re_f.append(m.ix[:,['orderid','end_loc1','end_loc2','end_loc3']])
        if nnn>500 and nnn - nnn/500*500==0:
            print nnn,sum(re_f['end_loc1'].isnull())
            print nnn,nul_v

    print sum(re_f['end_loc1'].isnull())
    re_f = re_f.reset_index(drop=True)
    re_f = re_f.drop(re_f[re_f.end_loc1==0].index,axis = 0)
#    re_f = re_f.drop(0)
    m_samplog = re_f['end_loc1'].isnull()
    nul_v = random.sample(re_f.ix[:,['end_loc1','end_loc2','end_loc3']].values.tolist(),1)
    re_f.ix[m_samplog,'end_loc1'] = nul_v[0][0]
    re_f.ix[m_samplog,'end_loc2'] = nul_v[0][1]
    re_f.ix[m_samplog,'end_loc3'] = nul_v[0][2]
    re_f.to_csv('split_data/'+str(num)+'results.csv',index=False)#,header = False)

num_split = 10000
start2end = pd.read_csv('testresults2.csv')
train_data = pd.read_csv('neaedis.csv')
testdata = read_testdata('test.csv')
near_grop = train_data.groupby('near_aear')

testd_reall = pd.merge(testdata,start2end,how = 'left',on = ['geohashed_start_loc','hoursection'])
print sum(testd_reall['end_loc1'].isnull())
n_s = len(testd_reall)/num_split
n_sorig = np.array(range(n_s))*num_split
n_send = [x+(num_split) for x in n_sorig]
for num_ in range(196,n_s):
    s_time = datetime.datetime.now()
    t_d_r = testd_reall[n_sorig[num_]:n_send[num_]]
    print len(t_d_r)
    print max(t_d_r.index),min(t_d_r.index)
    dealwithdata(t_d_r,num_,near_grop)
    e_time = datetime.datetime.now()
    print 'run time:',e_time-s_time
#t_d_r = testd_reall.ix[1990001:(len(testd_reall)-1),:]
t_d_r = testd_reall.ix[(n_send[-1]+1):(len(testd_reall)-1),:]
dealwithdata(t_d_r,n_s)
endtime = datetime.datetime.now()
print 'run time:',endtime-starttime
