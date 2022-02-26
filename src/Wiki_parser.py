"""
Parses a WikiBench trace, 
Output: a csv file, where:
        - the first col. is the keys,
        - the 2nd col. is the id of the clients of this req,
        - the rest of the cols. are the locations ("k_loc") to which a central controller would enter this req. upon a miss. 
"""


import numpy as np
import pandas as pd
import datetime as dt
import matplotlib as plt
import datetime, mmh3, sys, hashlib
import MyConfig 
from MyConfig import getTracesPath 
from numpy import infty

def parse_wiki_trace (trace_file_name      = 'wiki1.1190448987.txt',      
                      max_num_of_req       = MyConfig.INF_INT, # maximum number of requests to be parsed, starting from the beginning of the trace
                      max_trace_len_in_sec = MyConfig.INF_INT, # maximum time length to be parsed, starting from the beginning of the trace 
                      num_of_clients       = 3, 
                      num_of_locations     = 3):

    traces_path = getTracesPath()
    input_file_name = 'wiki/' + trace_file_name 
    df = pd.read_csv (traces_path + input_file_name, sep=' ', header=None, nrows = max_num_of_req, encoding = "ISO-8859-1")
        
    # take only "read" requests
    df = df.loc[df[3] == '-']
    
    # sort trace by time, breaking ties by sequence number
    df[1] = pd.to_datetime(df[1], unit='s')
    df = df.sort_values([1,0])
    df = df.reset_index(drop=True)

    if (max_trace_len_in_sec < float ('inf')):    
        df['time_diff'] = (df[1]-df.iloc[0][1]) # calculate time offset from the trace's beginning for every line in the trace  
        # under_Xm = (df['time_diff'] < datetime.timedelta(0, max_trace_len_in_sec, 0)) # boolean array of entries satisfying condition
        df = df[(df['time_diff'] < datetime.timedelta(0, max_trace_len_in_sec, 0))].copy()
    
    # generate hashes of URLs. name column 'key'
    unique_urls = np.unique(df[2])
    url_lut_dict = dict(zip(unique_urls , range(unique_urls.size))) # dictionary serving as a LUT associating each unique_url with a unique integer
    keys = np.array([url_lut_dict[url] for url in df[2]]).astype('uint32')
    
    # generate client assignments for each request
    client_assignment = np.random.RandomState(seed=42).randint(0 , num_of_clients , df.shape[0]).astype('uint8')
    
    # generate request ids
    req_id = np.array(range(df.shape[0])).astype('uint32')
    
    # generate permutation for each unique key in the trace
    unique_permutations_array  = np.array ([np.random.RandomState(seed=i).permutation(range(num_of_locations)) for i in range(unique_urls.size)]).astype('uint8')
    permutation_lut_dict = dict(zip(unique_urls , unique_permutations_array)) # generate dictionary to serve as a LUT of unique_key -> permutation
    # generate full permutations array for all requests. identical keys will get identical permutations
    permutations_array = np.array([permutation_lut_dict[url] for url in df[2]]).astype('uint8')
    permutations_df = pd.DataFrame(permutations_array)
    
    trace_df = pd.DataFrame(np.transpose([req_id, keys, client_assignment]))
    trace_df.columns = ['req_id', 'key', 'client_id']
    
    # # For pre-computation of all the hashes for each unique key in advance, instead of calculating them during sim-time, uncomment the lines below
    # hash_count = 5 # Assuming 5 hash functions
    # key_hash = []
    # seed = 0
    # key_hash0 = np.array( [mmh3.hash(key, seed) for key in keys])
    # key_hash = np.empty([hash_count, keys.size])
    # for seed in range(hash_count):
    # 	key_hash [seed, :] = np.array ([mmh3.hash(key, seed) for key in keys]).astype('uint32') 
    # trace_df = pd.DataFrame(np.transpose([req_id, keys, client_assignment, key_hash[0, :], key_hash[1, :], key_hash[2, :], key_hash[3, :], key_hash[4, :]]))
    # trace_df.columns = ['req_id', 'key', 'client_id', 'hash0', 'hash1', 'hash2', 'hash3', 'hash4']
    
    full_trace_df = pd.concat([ trace_df, permutations_df ], axis=1)
    
    full_trace_df.to_csv (traces_path + input_file_name.split (".txt")[0] + ".%dDSs.K%d.csv" %(num_of_clients, num_of_locations), index=False, header=True)
    
    ## validate string values in dataframe
    #non_string_keys = [i for i, key in enumerate(df[2]) if not isinstance(key,basestring)]
    #for i, key in enumerate(df[2].head()):
    #    if isinstance(key,basestring):
    #        print i
    
    ## historgram
    #count_df = trace.value_counts()
    #count_df[count_df > 1].plot()
    #hist_array = np.zeros(int(np.log2(count_df.max())) + 1)
    #for i in range(int(np.log2(count_df.max())) + 1):
    #    hist_array[i] = sum(count_df[(count_df >= 2**i) & (count_df < 2**(i+1))])

parse_wiki_trace ()
