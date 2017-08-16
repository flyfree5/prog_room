# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 10:24:35 2017

@author: fly
"""

import pandas as pd
import numpy as np
import scipy.stats as stats

def mylaglanri_dis(x1,x2):    
    return (12-abs(abs(x1-x2)-12))**2

def get_aver_delta(x_l):
    G={}
    for n_1i in range(24):
        for n_2i in range(10):
            n_ = n_1i+n_2i/10.0
            G[n_] = sum([mylaglanri_dis(n_,xx) for xx in x_l])
#    gg = G.values()
    x_ = sorted(G,key=lambda x:G[x])[0]
    delta = G[x_]/len(x_l)
    return x_,delta,G

def get_vonmises_results(t,u,ka):
    return stats.vonmises.pdf((t-u)/12*np.pi, ka)

def conditional_prob(p_xt,p_x,sumpxt):
    return p_xt*p_x/sumpxt

traindata = pd.read_csv('D:/mobai/data/train.csv')

traindata['starttime'] = pd.to_datetime(traindata['starttime'], format='%Y-%m-%d %H:%M:%S')
traindata['hour'] = [x.hour for x in traindata.starttime]
print traindata.head()
traindata_g = traindata.groupby('userid')
trdg_s = traindata_g.size()

a=traindata_g.get_group(1)
x_a,g_a,g = get_aver_delta(a.hour)
aa=a.groupby('geohashed_end_loc')
eig_v = {}
for name_endloc,endl_g in aa:
    if len(endl_g)>1:
        u_aa,delta_aa,g_aa = get_aver_delta(endl_g.hour)
#        get_vonmises_results(endl_g['hour'],u_aa,1/delta_aa)
        eig_v[name_endloc]=[u_aa,delta_aa,len(endl_g)/float(len(a))]
        
        
print x_a,g_a

