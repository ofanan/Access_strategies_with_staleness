import numpy as np
import mmh3
import BloomFilter
from tictoc import tic, toc
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size


req_df = gen_requests ('C:/Users/ofanan/Documents/traces/wiki/wiki1.1190448987_130K.csv', 20)
bf_old = BloomFilter.BloomFilter (size = 30, hash_count = 5)
#bf_new = BloomFilter.BloomFilter (size = 30, hash_count = 5)

tic()
for req_id in range(req_df.shape[0]):
	bf_old.add (req_df.iloc[req_id].key)
	# bf_new.add (req_df.iloc[req_id])
toc()  

	#cur_req = req_df.iloc[req_id] 
	#hashes = cur_req['hash%d'%0]
	#bf.add (cur_req.key, cur_req['hash%d'%0])

