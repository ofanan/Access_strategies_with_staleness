"""
A counting Bloom filter. for documentation, see:
http://www.maxburstein.com/blog/creating-a-simple-bloom-filter/
https://hur.st/bloomfilter/
http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
Bloom filter survey: https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf
"""
import numpy as np
import mmh3
import copy
import SimpleBloomFilter

class CountingBloomFilter(object):
    
    def __init__(self, size = 500, num_of_hashes = 5, max_array_val = 7):
        """
        Returns a CountingBloomFilter object with the following attributes:
            DS_size:        the maximal number of elements in the datastore (commonly referred to as $n$)
            bpe:            Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            num_of_hashes:     The CBF calculates the optimal number of hash funcs (commonly referred to as k) based on n and m
            max_array_val:  maximum value each counter can hold (default 7)
        """
        self.size               = size
        self.num_of_hashes         = num_of_hashes
        self.array              = np.zeros (self.size, dtype='uint8')
        self.max_array_val      = max_array_val
        #self.num_of_cntrs_set   = 0 # number of cntrs set since last update was sent 
        #self.num_of_cntrs_reset = 0 # number of cntrs set since last update was sent
        # self.num_of_sets_between_updates    = 200 
        # self.num_of_resets_between_updates  = 200
        #self.insertions_since_last_update   = 0 # number of insertions since last update was sent

    # def reset_delta_cntrs (self):
    #     self.num_of_cntrs_set               = 0 # number of cntrs set since last update was sent 
    #     self.num_of_cntrs_reset             = 0 # number of cntrs set since last update was sent        
    #     self.insertions_since_last_update   = 0


    def add(self, key):
        """
        adds a key to the Bloom filter
        updates all the hashes corresponding to the new key: increases each by 1 (no more than allowed maximum)
        """
        for seed in range(self.num_of_hashes):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] < self.max_array_val:
                self.array[entry] += 1
                # self.insertions_since_last_update += 1
                # if (self.array[entry] == 1):
                #     self.num_of_cntrs_set += 1


    def remove(self, key):
        """
        removes a key from the Bloom filter
        updates all the hashes corresponding to the key: decreases each by 1 (no less than 0)
        """
        for seed in range(self.num_of_hashes):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] > 0:
                self.array[entry] -= 1
                # if (self.array[entry] == 0):
                #     self.num_of_cntrs_reset += 1

    def __contains__(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        enables using the syntax:
            key in CountingBloomFilter
        """
        for seed in range(self.num_of_hashes):            
            if self.array[mmh3.hash(key, seed) % self.size] == 0:
                return False
        return True

    # def consider_send_update (self):
    #     if (self.num_of_cntrs_set >=  self.num_of_sets_between_updates or self.num_of_cntrs_reset >=  self.num_of_resets_between_updates):
    #         return True
    #     return False

    def gen_SimpleBloomFilter (self):
        """
        Returns a simple BF, which is the compression of this counting BF: each entry is True iff the respective entry in the CBF is > 0
        """
        sbf = SimpleBloomFilter.SimpleBloomFilter (size=self.size, num_of_hashes = self.num_of_hashes)
        for i in range (self.size):
            if (self.array[i] > 0):
                sbf.array[i] = True
        return sbf

    def lookup(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        """
        for seed in range(self.num_of_hashes):
            if self.array[mmh3.hash(key, seed) % self.size] == 0:
                return False
        return True
        


