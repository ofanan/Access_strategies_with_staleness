import numpy as np
import pandas as pd
import datetime as dt
import hashlib
import datetime
import mmh3
import sys
from collections import defaultdict

import MyConfig  

# Parses a trace whose format is merely a list of keys (each key in a different line). 
# Output: a csv file, where:
#         - the first col. is the keys,
#         - the 2nd col. is the id of the clients of this req,
#         - the rest of the cols. are the locations ("k_loc") to which a central controller would enter this req. upon a miss.
# input_file_name =  'wiki/wiki.1190448987.txt'
# input_file_name = 'gradle/gradle.build-cache_full.txt'
# input_file_name = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req.txt'
input_file_name = 'umass/storage/F2.3M_req.txt'
num_of_clients      = 3
kloc = 1
num_of_req = 1000000

traces_path = MyConfig.getTracesPath()
df = pd.read_csv (traces_path + input_file_name, sep=' ', header=None, nrows = num_of_req)


# generate hashes of URLs. name column 'keys'
unique_urls = np.unique (df)
#url_lut_dict = defaultdict(list)
url_lut_dict = dict(zip(unique_urls , range(unique_urls.size))) # generate dictionary to serve as a LUT of unique_key -> integer
keys = np.array ([url_lut_dict[url] for url in df[0]]).astype('uint32')

# generate client assignments for each request
client_assignment = np.random.RandomState(seed=42).randint(0 , num_of_clients , df.shape[0]).astype('uint8')

# generate request ids
req_id = np.array(range(df.shape[0])).astype('uint32')

unique_permutations_array  = np.array ([np.random.RandomState(seed=i).permutation(range(num_of_clients)) for i in range(unique_urls.size)]).astype('uint8') # generate permutation for each unique key in the trace
unique_permutations_array  = unique_permutations_array [:, range (kloc)] # Select only the first kloc columns for each row (unique key) in the random permutation matrix

print ('num of uniques = ', unique_permutations_array.size )

permutation_lut_dict = dict(zip(unique_urls , unique_permutations_array)) # generate dictionary to serve as a LUT of unique_key -> permutation

permutations_array = np.array([permutation_lut_dict[url] for url in df[0]]).astype('uint8') # generate full permutations array for all requests. identical keys will get identical permutations
permutations_df = pd.DataFrame(permutations_array)

trace_df = pd.DataFrame(np.transpose([req_id, keys, client_assignment]))
trace_df.columns = ['req_id', 'key', 'client_id']

# # For Calculating all the hashes for each unique key in advance, then during sim-time, uncomment the lines below
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

output_file_name = input_file_name.split (".txt")[0] + '_{:.0f}K_{:.0f}DSs.csv' .format (num_of_req/1000, num_of_clients)
full_trace_df.to_csv (traces_path + output_file_name, index=False, header=True)

## check memory space used by dataframe
#trace_df.info(memory_usage='deep')

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
