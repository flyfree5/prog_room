# -*- coding: utf-8 -*-
"""
Created on Tue Sep 05 10:01:52 2017

@author: fly
"""

import csv
import datetime

def load_reviews(path,**kwargs):
    options = {'fieldnames':('userid','movieid','rating','timestamp'),'delimiter':'\t'}
    options.update(kwargs)
    
    parse_date = lambda r,k:datetime.datetime.fromtimestamp(float(r[k]))
    parse_int = lambda r,k:int(r[k])
    
    with open(path,'rb') as reviews:
        reader = csv.DictReader(reviews,**options)
        for row in reader:
            row['userid'] = parse_int(row,'userid')
            row['movieid'] = parse_int(row,'movieid')
            row['rating'] = parse_int(row,'rating')
            row['timestamp'] = parse_date(row,'timestamp')
            yield row
            
import os

def relative_path(path):
    dirname = os.path.dirname(os.path.realpath('__file__'))
    path = os.path.join(dirname,path)
    return os.path.normpath(path)

def load_movies(path,**kwargs):
    options = {'fieldnames':('movieid','title','release','video','url'),
               'delimiter':'|','restkey':'genre'}
    options.update(kwargs)
    
    parse_int = lambda r,k:int(r[k])
    parse_date = lambda r,k:datetime.datetime.strptime(r[k],'%d-%b-%Y') if r[k] else None
    
    with open(path,'rb') as movies:
        reader = csv.DictReader(movies,**options)
        for row in reader:
            row['movieid'] = parse_int(row,'movieid')
            row['release'] = parse_date(row,'release')
            row['video'] = parse_date(row,'video')
            yield row

from collections import defaultdict
import heapq
from operator import itemgetter
import math

class MovieLens(object):
    def __init__(self,udata,uitem):
        self.udata = udata
        self.uitem = uitem
        self.movies = {}
        self.reviews = defaultdict(dict)
        self.load_dataset()
        
    def load_dataset(self):
        for movie in load_movies(self.uitem):
            self.movies[movie['movieid']] = movie
        for review in load_reviews(self.udata):
            self.reviews[review['userid']][review['movieid']]=review
    def reviews_for_movie(self,movieid):
        for review in self.reviews.values():
            if movieid in review:
                yield review[movieid]
                
    def average_reviews(self):
        for movieid in self.movies:
            reviews = list(r['rating'] for r in self.reviews_for_movie(movieid))
            average = sum(reviews)/float(len(reviews))
            yield (movieid,average,len(reviews))
            
    def bayesian_average(self,c=59,m=3):
        for movieid in self.movies:
            reviews = list(r['rating'] for r in self.reviews_for_movie(movieid))
            average = ((c*m)+sum(reviews))/float(c+len(reviews))
            yield (movieid,average,len(reviews))
            
    def top_rated(self,n=10):
        return heapq.nlargest(n,self.bayesian_average(),key=itemgetter(1))
    
    def shared_preferences(self,criticA,criticB):
        if criticA not in self.reviews:
            raise KeyError("Couldn't find critic '%s' in data" % criticA)
        if criticB not in self.reviews:
            raise KeyError("Couldn't find critic '%s' in data" % criticB)
            
        moviesA = set(self.reviews[criticA].keys())
        moviesB = set(self.reviews[criticB].keys())
        shared = moviesA & moviesB
        reviews = {}
        for movieid in shared:
            reviews[movieid] = (self.reviews[criticA][movieid]['rating'],
                   self.reviews[criticB][movieid]['rating'])
        return reviews
    
    def euclidean_distance(self,criticA,criticB):
        preferences = self.shared_preferences(criticA,criticB)
        if len(preferences)==0:
            return 0
        sum_of_squares = sum([pow(a-b,2) for a,b in preferences.values()])
        return 1/(1+math.sqrt(sum_of_squares))
    
    def pearson_correlation(self,criticA,criticB):
        preferences = self.shared_preferences(criticA,criticB)
        length = len(preferences)
        if length==0:
            return 0
        sumA = sumB = sumSquareA = sumSquareB = sumProducts = 0
        for a,b in preferences.values():
            sumA += a
            sumB += b
            sumSquareA += pow(a,2)
            sumSquareB += pow(b,2)
            sumProducts += a*b
        numerator = (sumProducts*length) - (sumA*sumB)
        denominator = math.sqrt(((sumSquareA*length) - pow(sumA,2))*((sumSquareB*length)-pow(sumB,2)))
        if denominator==0:
            return 0
        return abs(numerator/denominator)
    
    def similar_critics(self,user,metric='euclidean',n=None):
        metrics = {'euclidean':self.euclidean_distance,
                   'pearson':self.pearson_correlation}
        distance = metrics.get(metric,None)
        if user not in self.reviews:
            raise KeyError("Unknown user,'%s'." % user)
        if not distance or not callable(distance):
            raise KeyError("Unknown or unprogrammed distance metric '%s'." % metric)
        critics = {}
        for critic in self.reviews:
            if critic==user:
                continue
            critics[critic] = distance(user,critic)
            
        if n:
            return heapq.nlargest(n,critics.items(),key = itemgetter(1))
        return critics
    
    def predict_raking(self,user,movie,metric = 'euclidean',critics = None):
        critics = critics or self.similar_critics(user,metric = metric)
        total = 0.0
        simsum = 0.0
        for critic,similarity in critics.items():
            if movie in self.reviews[critic]:
                total += similarity*self.reviews[critic][movie]['rating']
                simsum += similarity
                
                
        if simsum==0.0:
            return 0.0
        return total/simsum
    
    def predict_all_rankings(self,user,metric='euclidean',n=None):
        critics = self.similar_critics(user,metric=metric)
        movies={
                movie:self.predict_raking(user,movie,metric,critics)
                for movie in self.movies
        }
        if n:
            return heapq.nlargest(n,movies.items(),key = itemgetter(1))
        return movies
    
    def shared_critics(self,movieA,movieB):
        if movieA not in self.movies:
            raise KeyError("Couldn't find movie '%s' in data" % movieA)
        if movieB not in self.movies:
            raise KeyError("Couldn't find movie '%s' in data" % movieB)
        criticsA = set(critic for critic in self.reviews if movieA in self.reviews[critic])
        criticsB = set(critic for critic in self.reviews if movieB in self.reviews[critic])
        shared = criticsA & criticsB
        
        reviews = {}
        for critic in shared:
            reviews[critic] = (
                    self.reviews[critic][movieA]['rating'],
                    self.reviews[critic][movieB]['rating'])
        return reviews
    
    def similar_items(self,movie,metric='euclidean',n=None):
        metrics={
                'euclidean':self.euclidean_distance,
                'pearson':self.pearson_correlation,
                }
        distance = metrics.get(metric,None)
        if movie not in self.reviews:
            raise KeyError("Unknown movie '%s'." % movie)
        if not distance or not callable(distance):
            raise KeyError("Unknown or unprogrammed distance metric '%s'." % metric)
        items={}
        for item in self.movies:
            if item==movie:
                continue
            items[item]=distance(item,movie,prefs='movies')
        if n:
            return heapq.nlargest(n,items.items(),key=itemgetter(1))
        return items
        
data = relative_path('ml-100k/u.data')
item = relative_path('ml-100k/u.item')
model = MovieLens(data,item)
#==============================================================================
# for mid,avg,num in model.top_rated(10):
#     title = model.movies[mid]['title']
#     print "[%0.3f average rating (%i reviews)] %s" % (avg,num,title)
#==============================================================================
#==============================================================================
# print model.pearson_correlation(232,532)
# 
# for item in model.similar_critics(232,'euclidean',n=10):
#     print "%4i: %0.3f" % item
# 
# for item in model.similar_critics(232,'pearson',n=10):
#     print "%4i: %0.3f" % item
#==============================================================================

print model.predict_raking(422,50,'euclidean')
print model.predict_raking(422,50,'pearson')
for mid,rating in model.predict_all_rankings(578,'pearson',10):
    print "%0.3f: %s" % (rating,model.movies[mid]['title'])
    
for movie,similarity in model.similar_items(631,'pearson').items():
    print "%0.3f: %s" % (similarity,model.movies[movie]['title'])
    
#            