import numpy as np
import mmh3
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size
import CountingBloomFilter
import SimpleBloomFilter


req_df = gen_requests ('C:/Users/ofanan/Documents/traces/wiki/wiki1.1190448987_130K.csv', 20)
cbf = CountingBloomFilter.CountingBloomFilter (size = 10, hash_count = 1)
sbf = cbf.gen_SimpleBloomFilter ()

cbf.add ("1")
print ("delta0 = ", cbf.calc_deltas(sbf)[0])
print ("delta1 = ", cbf.calc_deltas(sbf)[1])
cbf.add("2")
print ("delta0 = ", cbf.calc_deltas(sbf)[0])
print ("delta1 = ", cbf.calc_deltas(sbf)[1])
sbf = cbf.gen_SimpleBloomFilter ()
cbf.remove("1")
print ("delta0 = ", cbf.calc_deltas(sbf)[0])
print ("delta1 = ", cbf.calc_deltas(sbf)[1])
cbf.remove("2")
print ("delta0 = ", cbf.calc_deltas(sbf)[0])
print ("delta1 = ", cbf.calc_deltas(sbf)[1])


