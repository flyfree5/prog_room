# -*- coding: utf-8 -*-
"""
Created on Tue Aug 08 17:13:17 2017

@author: fly
"""

import pandas as pd
import os
import pygeohash

def read_data(name_):
    data_test = pd.read_csv('D:/mobai/data/trainstartloc/'+name_,header=None)
    data_test.columns = ['end_loc','times']
    return data_test
def get_filename(path):
    filenamelist = os.listdir(path)
    return [os.path.join(path,x) for x in filenamelist]

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
def get_endlocname(tlss_g,v):
    tl_gg = tlss_g.get_group(v)
    tile = tl_gg.index.levels[0]
    tila = tl_gg.index.labels[0]
    tlss_list = tile[tila].tolist()
    if len(tlss_list)==2:
        re = [tlss_list[0],tlss_list[0],tlss_list[1]]#.extend(0,tlss_list[0])
#        return re
    elif len(tlss_list)==1:
        re = [tlss_list[0]]*3
#        return [tlss_list[0]]*3
    elif len(tlss_list)==0:
        re = ['0']*3
    else:
        re = tlss_list[0:3]
    return re

traindata = read_testdata('D:/mobai/data/train.csv')

trainlocstart_grop = traindata.groupby('geohashed_start_loc')
start_l = []
end_l1 = []
end_l2 = []
end_l3 = []
hour_ = []
#traindic = {}
for name_trsg,trlsg in trainlocstart_grop:
    trgg = trlsg.groupby(['geohashed_end_loc','hoursection'])
    tls_s = trgg.size()
    tlss_g = tls_s.groupby(level = 'hoursection')
    for nm_tl,tlgg in tlss_g:
        tlgglist = get_endlocname(tlss_g,nm_tl)
        start_l.append(name_trsg)
        end_l1.append(tlgglist[0])
        end_l2.append(tlgglist[1])
        end_l3.append(tlgglist[2])
        hour_.append(nm_tl)
        print name_trsg,tlgglist,nm_tl
#        tlgglist.append(nm_tl)
#    traindic[name_trsg] = tlss_g
#end_l1 = [x[0] for x in end_l]
#end_l2 = [x[1] for x in end_l]
#end_l3 = [x[2] for x in end_l]
start2end = pd.DataFrame({'geohashed_start_loc':start_l,
                          'hoursection':hour_,
                          'end_loc1':end_l1,
                          'end_loc2':end_l2,
                          'end_loc3':end_l3})
start2end.to_csv('testresults.csv',index = False)

import random
start2end = pd.read_csv('testresults.csv')
testdata = read_testdata('D:/mobai/data/test.csv')
testd_re = pd.merge(testdata,start2end,how = 'left',on = ['geohashed_start_loc','hoursection'])
print sum(testd_re['end_loc1'].isnull())

testd_re_gr_s = testd_re.groupby('geohashed_start_loc')
for n,m in testd_re_gr_s:
    logi_end1 = pd.isnull(m.end_loc1)
    logi_n_end1 = pd.notnull(m.end_loc1)
    s_le = sum(logi_end1)
    if s_le>0 and s_le<len(m):
        m_samp = m.ix[logi_n_end1,['end_loc1','end_loc2','end_loc3']]
        nul_v = random.sample(m_samp.values.tolist(),1)
        m.ix[logi_end1,'end_loc1'] = nul_v[0]
        m.ix[logi_end1,'end_loc2'] = nul_v[1]
        m.ix[logi_end1,'end_loc3'] = nul_v[2]
        print nul_v
#    elif s_le==len(m)
#    for logi_i in logi_end1:
#        if
print sum(testd_re['end_loc1'].isnull())
re_f = testd_re.ix[:,['orderid','end_loc1','end_loc2','end_loc3']]
re_f.to_csv('results1.csv',index=False,header = False)

#testlocstart_grop = testdata.groupby('geohashed_start_loc')
#for nmtlg,telg in testlocstart_grop:
#    te = telg.groupby('hoursection')
#lat_lon = [pygeohash.decode_exactly(x) for x in testdata.geohashed_start_loc]
#testdata['lat'] = [x[0] for x in lat_lon]
#testdata['lon'] = [x[1] for x in lat_lon]

#testdata = read_testdata('D:/mobai/data/test.csv')
#testlocstart_grop = testdata.groupby('geohashed_start_loc')
#for nt,tlg in testlocstart_grop

#for n,m in tlss_g:
#    print n
#    print m
#    print get_endlocname(tlss_g,n)
    
    
    

