import numpy as np
import mmh3
import copy


class BloomFilter(object):
    
    def __init__(self, array, hash_count = 5):
        """
        Returns a SimpleBloomFilter object with the following attributes:
            array:          array of boolean (the bloom filter's array)
            hash_count:     number of hash functions to use for mapping each key (default 5)
        """
        self.hash_count         = hash_count
        self.array              = array
 
    def add(self, key):
        """
        adds a key to the Bloom filter
        updates all the hashes corresponding to the new key: increases each by 1 (no more than allowed maximum)
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            self.array[entry] = True

    def __contains__(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            if (!self.array[entry]):
                return False
        return True

