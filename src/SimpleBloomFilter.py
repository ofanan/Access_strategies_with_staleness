"""
A simple Bloom filter
"""


import numpy as np
import mmh3


class SimpleBloomFilter(object):
    
    def __init__(self, size, num_of_hashes):
        """
        Returns a SimpleBloomFilter object with the requested size and number of hash funcs
        """
        self.size       = size
        self.num_of_hashes = num_of_hashes
        self.array      = np.zeros(size, dtype='bool')
 
    def add_all (self, keys):
        """
        adds all the keys to the Bloom filter
        Set all the hashes corresponding to the new key
        """

    def add(self, key):
        """
        adds a key to the Bloom filter
        updates all the hashes corresponding to the new key: increases each by 1 (no more than allowed maximum)
        """
        for seed in range(self.num_of_hashes):
            self.array[mmh3.hash(key, seed) % self.size] = True

    def __contains__(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        enables using the syntax:
            key in SimpleBloomFilter
        """
        for seed in range(self.num_of_hashes):
            entry = mmh3.hash(key, seed) % self.size
            if ( not (self.array[entry]) ):
                return False
        return True

    def lookup(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        """
        for seed in range(self.num_of_hashes):
            if ( not (self.array[mmh3.hash(key, seed) % self.size]) ):
                return False
        return True
        

    def get_designed_fpr (self):
        """
        return the estimated designed false-positive ratio for the current load on the BF
        """
        return (sum(self.array) / self.size) ** self.num_of_hashes
