import numpy as np
import pandas as pd
import datetime as dt
import hashlib
import datetime
import mmh3
import sys
from collections import defaultdict

from MyConfig import getTracesPath 

# Parses a Gradle trace, or any other trace whose format is merely a list of keys (each key in a different line). 
# Output: a csv file, where:
#         - the first col. is the keys,
#         - the 2nd col. is the id of the clients of this req,
#         - the rest of the cols. are the locations ("k_loc") to which a central controller would enter this req. upon a miss.traces_path = getTracesPath()
# input_file_name = 'corda/corda.trace_vaultservice_50K.txt'
input_file_name = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req.txt'
num_of_clients      = 3
num_of_req = 400000

traces_path = getTracesPath()
df = pd.read_csv (traces_path + input_file_name, sep=' ', header=None, nrows = num_of_req)

num_of_locations = num_of_clients

# generate hashes of URLs. name column 'keys'
unique_urls = np.unique (df)
#url_lut_dict = defaultdict(list)
url_lut_dict = dict(zip(unique_urls , range(unique_urls.size))) # generate dictionary to serve as a LUT of unique_key -> integer
#keys = defaultdict(list)
# print (df[0])
# urls = np.array([df[0]])
# for url in urls:
# 	print (url)
keys = np.array ([url_lut_dict[url] for url in df[0]]).astype('uint32')

# generate client assignments for each request
client_assignment = np.random.RandomState(seed=42).randint(0 , num_of_clients , df.shape[0]).astype('uint8')

# generate request ids
req_id = np.array(range(df.shape[0])).astype('uint32')

# generate permutation for each unique key in the trace
unique_permutations_array  = np.array ([np.random.RandomState(seed=i).permutation(range(num_of_locations)) for i in range(unique_urls.size)]).astype('uint8')
permutation_lut_dict = dict(zip(unique_urls , unique_permutations_array)) # generate dictionary to serve as a LUT of unique_key -> permutation
# generate full permutations array for all requests. identical keys will get identical permutations
permutations_array = np.array([permutation_lut_dict[url] for url in df[0]]).astype('uint8')
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
