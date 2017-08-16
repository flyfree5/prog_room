# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 09:29:17 2017

@author: fly
"""

import pandas as pd
#import numpy as np
#import datetime

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

user_grop = traindata.groupby('userid')
#user_only = np.unique(traindata.userid)
user_id_ = []
hour_id_ = []
end_loc_ = []
print len(user_grop)
n=0
for uid_name,usr_df in user_grop:
    n+=1
    if n>500 and n-n/500*500==0:
        print n
#    user_id_ = uid_name
    hou_ug = usr_df.groupby('hoursection')
    for h_u_s,h_d in hou_ug:
        g_e_l = h_d.geohashed_end_loc.value_counts().index
        if len(g_e_l)==1:
            gel_ = [g_e_l[0]]*3
        elif len(g_e_l)==2:
            gel_ = [g_e_l[1],g_e_l[1],g_e_l[0]]
        else:
            gel_ = [g_e_l[-1],g_e_l[-2],g_e_l[-3]]
        print gel_
        user_id_.append(uid_name)
        hour_id_.append(h_u_s)
        end_loc_.append(gel_)

end_loc1_ = [x[0] for x in end_loc_]
end_loc2_ = [x[1] for x in end_loc_]
end_loc3_ = [x[2] for x in end_loc_]
start2end_user = pd.DataFrame({'userid':user_id_,
                          'hoursection':hour_id_,
                          'end_loc':end_loc_})
if sum(start2end_user['end_loc'].isnull())==0:
    start2end_user.to_csv('user_pre.csv',index = False)


#
