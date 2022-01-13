"""
Checks the hit rate in a given cache trace.
"""

import os
import numpy as np
import pandas as pd
import datetime
import sys
import pickle
from   printf import printf
from   tictoc import tic, toc
import mod_pylru
import itertools

import python_simulator as sim
from MyConfig import getTracesPath 
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size

traces_path         = getTracesPath()
# trace_file_name     = 'wiki/wiki1.1190448987_50K.3DSs.K3.csv'
# trace_file_name     = 'gradle/gradle.build-cache_50K_3DSs.csv'
trace_file_name     = 'corda/corda.trace_vaultservice_50K_3DSs.csv'
max_num_of_req = 50000

size = 100
num_of_DSs = 3
k_loc = 1
requests            = gen_requests (traces_path + trace_file_name, max_num_of_req, num_of_DSs)
DS = [mod_pylru.lrucache (size) for i in range (num_of_DSs)]# LRU cache. for documentation, see: https://pypi.org/project/pylru/
hit_cnt = 0
miss_cnt = 0
for req_id in range(requests.shape[0]): # for each request in the trace...
    cur_req = requests.iloc[req_id] 
    key = cur_req.key
    hits_list = np.array ([DS_id for DS_id in range(num_of_DSs) if key in DS[DS_id] ])   
    if (hits_list.size == 0): #miss
        miss_cnt = miss_cnt + 1
        for k in range (k_loc): #Insert key to all the (randomly chosen) k_loc locations
            DS[cur_req['%d'%k]][key] = key
    else: # hit
        hit_cnt = hit_cnt + 1
        DS[hits_list[0]][key] # Touch the key, to indicate that it was accessed
print ('hit cnt = {}, miss cnt = {}\n' .format (hit_cnt, miss_cnt))
print ('hit rate = {:.2}\n' .format (hit_cnt / (hit_cnt + miss_cnt)))
                
