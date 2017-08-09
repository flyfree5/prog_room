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

traindata = read_testdata('D:/mobai/data/train.csv')
testdata = read_testdata('D:/mobai/data/test.csv')

#lat_lon = [pygeohash.decode_exactly(x) for x in testdata.geohashed_start_loc]
#testdata['lat'] = [x[0] for x in lat_lon]
#testdata['lon'] = [x[1] for x in lat_lon]

#testdata = read_testdata('D:/mobai/data/test.csv')
#testlocstart_grop = testdata.groupby('geohashed_start_loc')
#for nt,tlg in testlocstart_grop:
    

